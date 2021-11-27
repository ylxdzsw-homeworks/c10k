#define _GNU_SOURCE

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <getopt.h>

#define REQUEST_MSG_LEN 20
#define RESPONSE_MSG_LEN 512

#define PENDING_REQUEST_QUEUE 1024
#define EPOLL_EVENT_BUFFER_SIZE 1024
#define SERVER_STARTING_PORT 10000

static unsigned short n_threads = 4;
static unsigned short n_server_ports = 16;
static const char *request_msg;
static const char *response_msg;
static const char *server_address;

void panic(const char *const msg) {
    perror(msg);
    exit(1);
}

int main(const int argc, char *const argv[]) {
    char role;
    int opt;

    while ((opt = getopt(argc, argv, "sct:p:a:")) != -1) {
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
                server_address = optarg; // copy the pointer
                break;
            default:
                printf("Parsing commandline options failed");
                exit(2);
        }
    }

    if (n_server_ports % n_threads)
        panic("cannot divide server ports");

    switch (role) {
        case 's':
            
            response_msg = malloc(RESPONSE_MSG_LEN); // leaked
            break;
        case 'c':
            request_msg = malloc(REQUEST_MSG_LEN); // leaked
            break;
    }
}

// setup the listening sockets
void server_setup_listeners(const unsigned short rank, int *const listener_fds) {
    struct sockaddr_in sockaddr;

    const unsigned short num_ports = n_server_ports / n_threads;
    const unsigned short port_offset = SERVER_STARTING_PORT + rank * num_ports;

    for (unsigned short i = 0; i < num_ports; i++) {
        const int listener_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_fd < 0)
            panic("Error creating socket");

        memset((char *)&sockaddr, 0, sizeof(sockaddr));
        sockaddr.sin_family = AF_INET;
        sockaddr.sin_port = htons(port_offset + i);
        sockaddr.sin_addr.s_addr = INADDR_ANY;

        if (bind(listener_fd, (struct sockaddr *)&sockaddr, sizeof(struct sockaddr_in)) < 0)
            panic("Error binding socket");

        if (listen(listener_fd, PENDING_REQUEST_QUEUE) < 0)
            panic("Error listening");

        listener_fds[i] = listener_fd;
    }
}

enum status {
    status_listening, status_reading, status_writing
};

struct event_data {
    enum status status;
    int fd;
    int bytes_remaining;
};

// the entry of a server thread
int server_run(const unsigned short rank) {
    const unsigned short num_ports = n_server_ports / n_threads;

    int *const listener_fds = malloc(sizeof(int) * num_ports); // leaked
    server_setup_listeners(rank, listener_fds);

    int epoll_fd = epoll_create(EPOLL_EVENT_BUFFER_SIZE); // a single epoll for a thread. The size parameter is unused in Linux
    if (epoll_fd < 0)
        panic("Error creating epoll");

    for (int i = 0; i < num_ports; i++) {
        struct event_data *const event_data = malloc(sizeof(struct event_data)); // leaked
        event_data->status = status_listening;
        event_data->fd = listener_fds[i];

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.ptr = event_data;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listener_fds[i], &ev) < 0)
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

                    struct event_data *const event_data = malloc(sizeof(struct event_data));
                    event_data->status = status_reading;
                    event_data->fd = socket_fd;
                    event_data->bytes_remaining = REQUEST_MSG_LEN;

                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.ptr = event_data;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &ev) < 0)
                        panic("Error adding socket to epoll");
                    break;

                case status_reading:
                    char buffer[REQUEST_MSG_LEN];
                    const int bytes_read = recv(event_data->fd, buffer, REQUEST_MSG_LEN, 0);

                    if (bytes_read < 0) {
                        fprintf(stderr, "WARNING: connection closed by client\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        free(event_data);
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_read) { // request finished, now write response
                        struct epoll_event ev;
                        ev.events = EPOLLOUT;
                        ev.data.ptr = event_data;
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, event_data->fd, &ev);
                        event_data->status = status_writing;
                        event_data->bytes_remaining = RESPONSE_MSG_LEN;
                    } else { // not finished, continue waiting
                        event_data->bytes_remaining -= bytes_read;
                    }
                    break;

                case status_writing:
                    const char *msg = response_msg + RESPONSE_MSG_LEN - event_data->bytes_remaining;
                    const int bytes_written = send(event_data->fd, msg, RESPONSE_MSG_LEN - event_data->bytes_remaining, 0);

                    if (bytes_written < 0) {
                        fprintf(stderr, "WARNING: connection closed by client\n");
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        free(event_data);
                        continue;
                    }

                    if (event_data->bytes_remaining <= bytes_written) { // response finished, closing the socket
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, event_data->fd, NULL);
                        shutdown(event_data->fd, SHUT_RDWR);
                        free(event_data);
                    } else { // not finished, continue writing
                        event_data->bytes_remaining -= bytes_written;
                    }
                    break;
            }
        }
    }
}

