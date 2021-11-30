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
#define EPOLL_EVENT_BUFFER_SIZE 256 // one page
#define SERVER_STARTING_PORT 10000

static unsigned short n_threads = 4;
static unsigned short n_server_ports = 16;
static unsigned long n_connections = 10240;
static in_addr_t server_address;

static volatile unsigned char closing = 0; // client will close the connections when receiving response if closing become 1.
static const char *request_msg;
static const char *response_msg;
static pthread_barrier_t barrier;

void panic(const char *const msg) {
    perror(msg);
    exit(1);
}

enum status {
    status_listening, status_reading, status_writing
};

struct event_data {
    enum status status;
    int fd;
    int bytes_remaining;

    struct {
        unsigned long long last_start_time;
        unsigned int sessions;
        unsigned int running_average_latency;
    } statistics;
};

int server_listen(const unsigned short port) {
    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0)
        panic("Error creating socket");

    const int reuse = 1;
    setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in sockaddr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(socket_fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) < 0)
        panic("Error binding socket");

    if (listen(socket_fd, TCP_REQUEST_QUEUE) < 0)
        panic("Error listening");

    return socket_fd;
}

int server_initialize_epoll(const unsigned short rank) {
    const unsigned short num_ports = n_server_ports / n_threads;
    const unsigned short port_offset = SERVER_STARTING_PORT + rank * num_ports;

    const int epoll_fd = epoll_create(EPOLL_EVENT_BUFFER_SIZE); // a single epoll for a thread. The size parameter is unused in Linux
    if (epoll_fd < 0)
        panic("Error creating epoll");

    for (int i = 0; i < num_ports; i++) {
        const int socket_fd = server_listen(port_offset + i);

        struct event_data *const event_data = malloc(sizeof(struct event_data)); // leaked
        event_data->status = status_listening;
        event_data->fd = socket_fd;

        struct epoll_event ev = {
            .events = EPOLLIN,
            .data.ptr = event_data
        };

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
            panic("Error adding listener to epoll");
    }

    return epoll_fd;
}

void server_handle_listen(const int epoll_fd, struct event_data *const event_data) {
    const int socket_fd = accept4(event_data->fd, NULL, NULL, SOCK_NONBLOCK);
    if (socket_fd < 0)
        panic("Error accepting connection");

    struct event_data *const event_data_conn = malloc(sizeof(struct event_data));
    event_data_conn->status = status_reading;
    event_data_conn->fd = socket_fd;
    event_data_conn->bytes_remaining = REQUEST_MSG_LEN;

    struct epoll_event ev = {
        .events = EPOLLIN,
        .data.ptr = event_data_conn
    };

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
        panic("Error adding socket to epoll");
}

void server_handle_read(const int epoll_fd, struct event_data *const event_data) {
    char buffer[REQUEST_MSG_LEN];
    const int bytes_read = recv(event_data->fd, buffer, event_data->bytes_remaining, 0);

    if (bytes_read <= 0) {
        if (event_data->bytes_remaining < REQUEST_MSG_LEN)
            fprintf(stderr, "WARNING: reading failed, connection closed by client\n");
        close(event_data->fd); // This automatically remove it from the epoll and send FIN.
        free(event_data);
        return;
    }

    if (event_data->bytes_remaining <= bytes_read) { // request finished, now write response
        struct epoll_event ev = {
            .events = EPOLLOUT,
            .data.ptr = event_data
        };

        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev) < 0)
            panic("Error modifying interest in epoll");
        event_data->status = status_writing;
        event_data->bytes_remaining = RESPONSE_MSG_LEN;
    } else { // not finished, continue waiting
        event_data->bytes_remaining -= bytes_read;
    }
}

void server_handle_write(const int epoll_fd, struct event_data *const event_data) {
    const char *msg = response_msg + RESPONSE_MSG_LEN - event_data->bytes_remaining;
    const int bytes_written = send(event_data->fd, msg, event_data->bytes_remaining, 0);

    if (bytes_written <= 0) {
        fprintf(stderr, "WARNING: writing failed, connection closed by client\n");
        close(event_data->fd);
        free(event_data);
        return;
    }

    if (event_data->bytes_remaining <= bytes_written) { // response finished, now write the next response
        struct epoll_event ev = {
            .events = EPOLLIN,
            .data.ptr = event_data
        };

        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev) < 0)
            panic("Error modifying interest in epoll");
        event_data->status = status_reading;
        event_data->bytes_remaining = REQUEST_MSG_LEN;
    } else { // not finished, continue writing
        event_data->bytes_remaining -= bytes_written;
    }
}

void server_poll(const int epoll_fd) {
    struct epoll_event event_buffer[EPOLL_EVENT_BUFFER_SIZE];

    while (1) {
        const int n_events = epoll_wait(epoll_fd, event_buffer, EPOLL_EVENT_BUFFER_SIZE, -1);

        if (n_events < 0)
            panic("Epoll Error");

        for (int i = 0; i < n_events; i++) {
            struct event_data *const event_data = event_buffer[i].data.ptr;
            switch (event_data->status) {
                case status_listening: ;
                    server_handle_listen(epoll_fd, event_data);
                    break;

                case status_reading: ;
                    server_handle_read(epoll_fd, event_data);
                    break;

                case status_writing: ;
                    server_handle_write(epoll_fd, event_data);
                    break;
            }
        }
    }
}

void *server_thread_entry(void *ptr) {
    const unsigned short rank = (unsigned short) (size_t) ptr;
    const int epoll_fd = server_initialize_epoll(rank);
    server_poll(epoll_fd);
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

    struct sockaddr_in sockaddr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = server_address
    };

    if (connect(socket_fd, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) < 0)
        panic("Error connecting");

    return socket_fd;
}

int client_initialize_epoll(const unsigned long n_connections_local) {
    const int epoll_fd = epoll_create(EPOLL_EVENT_BUFFER_SIZE);
    if (epoll_fd < 0)
        panic("Error creating epoll");

    for (int i = 0; i < n_connections_local; i++) {
        const int socket_fd = client_connect(SERVER_STARTING_PORT + i % n_server_ports);

        struct event_data *const event_data = malloc(sizeof(struct event_data));
        event_data->status = status_writing;
        event_data->fd = socket_fd;
        event_data->bytes_remaining = REQUEST_MSG_LEN;

        struct epoll_event ev = {
            .events = EPOLLOUT,
            .data.ptr = event_data
        };

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
            panic("Error adding to epoll");
    }

    return epoll_fd;
}

void client_handle_write(const int epoll_fd, struct event_data *const event_data, unsigned long *const remaining_connections) {
    const char *msg = request_msg + REQUEST_MSG_LEN - event_data->bytes_remaining;
    const int bytes_written = send(event_data->fd, msg, event_data->bytes_remaining, 0);

    if (bytes_written <= 0) {
        fprintf(stderr, "WARNING: writing failed, connection closed by server\n");
        close(event_data->fd);
        free(event_data);
        *remaining_connections -= 1;
        return;
    }

    if (event_data->bytes_remaining <= bytes_written) { // request finished, now write response
        struct epoll_event ev = {
            .events = EPOLLIN,
            .data.ptr = event_data
        };
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev);
        event_data->status = status_reading;
        event_data->bytes_remaining = RESPONSE_MSG_LEN;
    } else { // not finished, continue waiting
        event_data->bytes_remaining -= bytes_written;
    }
}

void client_handle_read(const int epoll_fd, struct event_data *const event_data, unsigned long *const remaining_connections) {
    char buffer[RESPONSE_MSG_LEN];
    const int bytes_read = recv(event_data->fd, buffer, event_data->bytes_remaining, 0);

    if (bytes_read <= 0) {
        fprintf(stderr, "WARNING: reading failed, connection closed by server\n");
        close(event_data->fd);
        free(event_data);
        *remaining_connections -= 1;
        return;
    }

    if (event_data->bytes_remaining <= bytes_read) { // finished this session
        if (closing) {
            close(event_data->fd);
            free(event_data);
            *remaining_connections -= 1;
        } else { // start next session
            struct epoll_event ev = {
                .events = EPOLLOUT,
                .data.ptr = event_data
            };
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev);
            event_data->status = status_writing;
            event_data->bytes_remaining = REQUEST_MSG_LEN;
        }
    } else { // not finished, continue reading
        event_data->bytes_remaining -= bytes_read;
    }
}

void client_run(const unsigned short rank) {
    const unsigned long n_connections_local = n_connections / n_threads;
    const int epoll_fd = client_initialize_epoll(n_connections_local);

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
                case status_writing: ;
                    client_handle_write(epoll_fd, event_data, &remaining_connections);
                    break;

                case status_reading: ;
                    client_handle_read(epoll_fd, event_data, &remaining_connections);
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

    const unsigned long long staring_time = get_time_millis();
    pthread_barrier_init(&barrier, NULL, n_threads + 1); // worker threads as well as this thread. The barrier is leaked. (no destroying)
    fprintf(stderr, "initiating connections\n");

    pthread_t *const threads = malloc(sizeof(pthread_t) * n_threads); // leaked
    for (unsigned short i = 0; i < n_threads; i++) {
        const int error = pthread_create(threads + i, NULL, client_thread_entry, (void *) (size_t) i);
        if (error) panic("Error spawning thread");
    }

    pthread_barrier_wait(&barrier);
    fprintf(stderr, "connections established. elapsed: %lldms\n", get_time_millis() - staring_time);

    const char is_oneshot = closing;

    if (!is_oneshot) {
        sleep(10);
        closing = 1; // we only do simple store and load (no fetch_xx or CAS), so no need for atomics
    }

    for (unsigned short i = 0; i < n_threads; i++)
        pthread_join(threads[i], NULL);

    if (is_oneshot) {
        fprintf(stderr, "finished. elapsed: %lldms\n", get_time_millis() - staring_time);
    } else {
        // TODO: report statistics about each connection
    }
}

int main(const int argc, char *const argv[]) {
    char role = 'h';
    int opt;

    while ((opt = getopt(argc, argv, "sct:p:a:n:oh")) != -1) {
        switch (opt) {
            case 's': // server
                role = 's';
                break;
            case 'c': // client
                role = 'c';
                break;
            case 't': // thread
                n_threads = atoi(optarg);
                break;
            case 'p': // ports
                n_server_ports = atoi(optarg);
                break;
            case 'a': // address
                server_address = inet_addr(optarg);
                break;
            case 'n': // number of connections
                n_connections = atoi(optarg);
                break;
            case 'o': // one-shot
                closing = 1;
                break;
            case 'h': // help
                role = 'h';
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
            fprintf(stderr,
                "Usage: epoll [options]\n"
                " -s: run as server\n"
                " -c: run as client\n"
                " -t <thread>: number of threads\n"
                " -p <ports>: number of server ports\n"
                " -a <address>: server address (client-only)\n"
                " -n <connections>: number of connections (client-only)\n"
                " -o: each connection only sends request once\n"
                " -h: show this help message\n"
            );
    }
}