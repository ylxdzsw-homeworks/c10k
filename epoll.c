#define _GNU_SOURCE

#include <pthread.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <getopt.h>

#define REQUEST_MSG_LEN 20
#define RESPONSE_MSG_LEN 512

#define TCP_REQUEST_QUEUE 1024
#define EPOLL_EVENT_BUFFER_SIZE 256 // one page long
#define SERVER_STARTING_PORT 10000

static unsigned short n_threads = 4;
static unsigned short n_server_ports = 16;
static unsigned long n_connections = 10000;
static const char *request_msg;
static const char *response_msg;
static in_addr_t server_address;
static pthread_barrier_t barrier;

void panic(const char *const msg) {
    perror(msg);
    exit(1);
}

int server_listen(const unsigned short port) {
    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0)
        panic("Error creating socket");

    const int reuse = 1;
    setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in sockaddr = {0};
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    sockaddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(socket_fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) < 0)
        panic("Error binding socket");

    if (listen(socket_fd, TCP_REQUEST_QUEUE) < 0)
        panic("Error listening");

    return socket_fd;
}

enum status {
    status_listening, status_reading, status_writing
};

struct event_data {
    enum status status;
    int fd;
    int bytes_remaining;
};

void server_run(const unsigned short rank) {
    const unsigned short num_ports = n_server_ports / n_threads;
    const unsigned short port_offset = SERVER_STARTING_PORT + rank * num_ports;

    int epoll_fd = epoll_create(EPOLL_EVENT_BUFFER_SIZE); // a single epoll for a thread. The size parameter is unused in Linux
    if (epoll_fd < 0)
        panic("Error creating epoll");

    for (int i = 0; i < num_ports; i++) {
        int socket_fd = server_listen(port_offset + i);

        struct event_data *const event_data = malloc(sizeof(struct event_data)); // leaked
        event_data->status = status_listening;
        event_data->fd = socket_fd;

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.ptr = event_data;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
            panic("Error adding listener to epoll");
    }

    struct epoll_event event_buffer[EPOLL_EVENT_BUFFER_SIZE];

    while (1) {
        const int n_events = epoll_wait(epoll_fd, event_buffer, EPOLL_EVENT_BUFFER_SIZE, -1);

        if (n_events < 0)
            panic("Epoll Error");

        for (int i = 0; i < n_events; i++) {
            struct event_data *const event_data = event_buffer[i].data.ptr;
            switch (event_data->status) {
                case status_listening:
                    const int socket_fd = accept4(event_data->fd, NULL, NULL, SOCK_NONBLOCK);
                    if (socket_fd < 0)
                        panic("Error accepting connection");

                    struct event_data *const event_data_conn = malloc(sizeof(struct event_data)); // freed when the connection closed
                    event_data_conn->status = status_reading;
                    event_data_conn->fd = socket_fd;
                    event_data_conn->bytes_remaining = REQUEST_MSG_LEN;

                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.ptr = event_data_conn;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
                        panic("Error adding socket to epoll");
                    break;

                case status_reading:
                    char buffer[REQUEST_MSG_LEN];
                    const int bytes_read = recv(event_data->fd, buffer, event_data->bytes_remaining, 0);

                    if (bytes_read <= 0) {
                        fprintf(stderr, "WARNING: reading failed, connection closed by client\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        close(event_data->fd);
                        free(event_data);
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_read) { // request finished, now write response
                        struct epoll_event ev;
                        ev.events = EPOLLOUT;
                        ev.data.ptr = event_data;
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev);
                        shutdown(event_data->fd, SHUT_RD);
                        event_data->status = status_writing;
                        event_data->bytes_remaining = RESPONSE_MSG_LEN;
                    } else { // not finished, continue waiting
                        event_data->bytes_remaining -= bytes_read;
                    }
                    break;

                case status_writing:
                    const char *msg = response_msg + RESPONSE_MSG_LEN - event_data->bytes_remaining;
                    const int bytes_written = send(event_data->fd, msg, event_data->bytes_remaining, 0);

                    if (bytes_written <= 0) {
                        fprintf(stderr, "WARNING: writing failed, connection closed by client\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        close(event_data->fd);
                        free(event_data);
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_written) { // response finished, closing the socket
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_WR);
                        close(event_data->fd);
                        free(event_data);
                    } else { // not finished, continue writing
                        event_data->bytes_remaining -= bytes_written;
                    }
                    break;
            }
        }
    }
}

void *server_thread_entry(void *ptr) {
    const unsigned short rank = (unsigned short) (size_t) ptr;
    server_run(rank);
    return NULL;
}

void server_entry() {
    response_msg = malloc(RESPONSE_MSG_LEN); // leaked
    // strcpy(response_msg, "response");

    pthread_t *const threads = malloc(sizeof(pthread_t) * n_threads); // leaked
    for (unsigned short i = 0; i < n_threads; i++) {
        int error = pthread_create(threads + i, NULL, server_thread_entry, (void *) (size_t) i);
        if (error) panic("Error spawning thread");
    }

    for (unsigned short i = 0; i < n_threads; i++) {
        pthread_join(threads[i], NULL);
    }
}

unsigned long long get_time_millis() {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts))
        panic("clock_gettime failed");
    return (unsigned long long) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int client_connect(const unsigned short port) {
    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0)
        panic("Error creating socket");

    struct sockaddr_in sockaddr = {0};
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    sockaddr.sin_addr.s_addr = server_address;

    if (connect(socket_fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) < 0)
        panic("Error connecting");

    return socket_fd;
}

void client_run(const unsigned short rank) {
    const unsigned long n_connections_local = n_connections / n_threads;

    int epoll_fd = epoll_create(EPOLL_EVENT_BUFFER_SIZE);
    if (epoll_fd < 0)
        panic("Error creating epoll");

    for (int i = 0; i < n_connections_local; i++) {
        int socket_fd = client_connect(SERVER_STARTING_PORT + i % n_server_ports);

        struct event_data *const event_data = malloc(sizeof(struct event_data)); // freeed when connection closed
        event_data->status = status_writing;
        event_data->fd = socket_fd;
        event_data->bytes_remaining = REQUEST_MSG_LEN;

        struct epoll_event ev;
        ev.events = EPOLLOUT;
        ev.data.ptr = event_data;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
            panic("Error adding to epoll");
    }

    pthread_barrier_wait(&barrier);

    struct epoll_event event_buffer[EPOLL_EVENT_BUFFER_SIZE];
    unsigned long remaining_connections = n_connections_local;

    while (remaining_connections) {
        const int n_events = epoll_wait(epoll_fd, event_buffer, EPOLL_EVENT_BUFFER_SIZE, -1);

        if (n_events < 0)
            panic("Epoll Error");

        for (int i = 0; i < n_events; i++) {
            struct event_data *const event_data = event_buffer[i].data.ptr;
            switch (event_data->status) {
                case status_writing:
                    const char *msg = request_msg + REQUEST_MSG_LEN - event_data->bytes_remaining;
                    const int bytes_written = send(event_data->fd, msg, event_data->bytes_remaining, 0);

                    if (bytes_written <= 0) {
                        fprintf(stderr, "WARNING: writing failed, connection closed by server\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        close(event_data->fd);
                        free(event_data);
                        remaining_connections -= 1;
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_written) { // request finished, now write response
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = event_data;
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev);
                        shutdown(event_data->fd, SHUT_WR);
                        event_data->status = status_reading;
                        event_data->bytes_remaining = RESPONSE_MSG_LEN;
                    } else { // not finished, continue waiting
                        event_data->bytes_remaining -= bytes_written;
                    }
                    break;

                case status_reading:
                    char buffer[RESPONSE_MSG_LEN];
                    const int bytes_read = recv(event_data->fd, buffer, event_data->bytes_remaining, 0);

                    if (bytes_read <= 0) {
                        fprintf(stderr, "WARNING: reading failed, connection closed by server\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        close(event_data->fd);
                        free(event_data);
                        remaining_connections -= 1;
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_read) {
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RD);
                        close(event_data->fd);
                        free(event_data);
                        remaining_connections -= 1;
                    } else { // not finished, continue reading
                        event_data->bytes_remaining -= bytes_read;
                    }
                    break;
            }
        }
    }
}

void *client_thread_entry(void *ptr) {
    const unsigned short rank = (unsigned short) (size_t) ptr;
    client_run(rank);
    return NULL;
}

void client_entry() {
    request_msg = malloc(REQUEST_MSG_LEN); // leaked
    // strcpy(request_msg, "request");

    unsigned long long staring_time = get_time_millis();
    pthread_barrier_init(&barrier, NULL, n_threads + 1); // worker threads as well as this thread. The barrier is leaked. (no destroying)
    printf("initiating connections\n");

    pthread_t *const threads = malloc(sizeof(pthread_t) * n_threads); // leaked
    for (unsigned short i = 0; i < n_threads; i++) {
        int error = pthread_create(threads + i, NULL, client_thread_entry, (void *) (size_t) i);
        if (error) panic("Error spawning thread");
    }

    pthread_barrier_wait(&barrier);
    printf("connections established. elapsed: %dms\n", get_time_millis() - staring_time);

    for (unsigned short i = 0; i < n_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("finished. elapsed: %dms\n", get_time_millis() - staring_time);
}

int main(const int argc, char *const argv[]) {
    char role = 'h';
    int opt;

    while ((opt = getopt(argc, argv, "sct:p:a:n:h")) != -1) {
        switch (opt) {
            case 's':
                role = 's';
                break;
            case 'c':
                role = 'c';
                break;
            case 't':
                n_threads = atoi(optarg);
                break;
            case 'p':
                n_server_ports = atoi(optarg);
                break;
            case 'a':
                server_address = inet_addr(optarg);
                break;
            case 'n':
                n_connections = atoi(optarg);
                break;
            case 'h':
                role = 'h'; // for help
                break;
            default:
                panic("Parsing commandline options failed");
        }
    }

    switch (role) {
        case 's':
            if (n_server_ports % n_threads)
                panic("cannot divide server ports");

            server_entry();
            break;
        case 'c':
            if (n_connections % n_server_ports)
                panic("cannot evenly distribute connections to server ports");
            if (n_connections % n_threads)
                panic("cannot evenly distribute connections to threads");

            client_entry();
            break;
        case 'h':
        default:
            printf(
                "Usage: epoll [options]\n"
                " -s: run as server\n"
                " -c: run as client\n"
                " -t <thread>: number of threads\n"
                " -p <ports>: number of server ports\n"
                " -a <address>: server address (client-only)\n"
                " -n <connections>: number of connections (client-only)\n"
                " -h: show this help message\n"
            );
    }
}