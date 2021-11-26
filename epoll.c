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
#define MAX_EVENTS 128
#define MAX_MESSAGE_LEN 2048

static const socklen_t socklen = sizeof(struct sockaddr_in);
static const unsigned short server_starting_port = 10000;

static unsigned short n_threads = 4;
static unsigned short n_server_ports = 16;
static const char *request_msg;
static const char *response_msg;
static const char *server_address;

void panic(const char *const msg) {
    perror(msg);
    exit(1);
}

int main(const int argc, const char *const argv[]) {
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
    const unsigned short port_offset = server_starting_port + rank * num_ports;

    for (unsigned short i = 0; i < num_ports; i++) {
        const int listener_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_fd < 0)
            panic("Error creating socket");

        memset((char *)&sockaddr, 0, sizeof(sockaddr));
        sockaddr.sin_family = AF_INET;
        sockaddr.sin_port = htons(port_offset + i);
        sockaddr.sin_addr.s_addr = INADDR_ANY;

        if (bind(listener_fd, (struct sockaddr *)&sockaddr, socklen) < 0)
            panic("Error binding socket");

        if (listen(listener_fd, PENDING_REQUEST_QUEUE) < 0)
            error("Error listening");

        listener_fds[i] = listener_fd;
    }
}

// the entry of a server thread
int server_run(const unsigned short rank) {
    int *const listener_fds = malloc()

    int portno = 10000;
    struct sockaddr_in server_addr, client_addr;

    char buffer[MAX_MESSAGE_LEN];
    memset(buffer, 0, sizeof(buffer));


    struct epoll_event ev, events[MAX_EVENTS];
    int new_events, sock_conn_fd, epollfd;

    epollfd = epoll_create(MAX_EVENTS);
    if (epollfd < 0)
    {
        error("Error creating epoll..\n");
    }
    ev.events = EPOLLIN;
    ev.data.fd = sock_listen_fd;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock_listen_fd, &ev) == -1)
    {
        error("Error adding new listeding socket to epoll..\n");
    }

    while (1)
    {
        new_events = epoll_wait(epollfd, events, MAX_EVENTS, -1);

        if (new_events == -1)
        {
            error("Error in epoll_wait..\n");
        }

        for (int i = 0; i < new_events; ++i)
        {
            if (events[i].data.fd == sock_listen_fd)
            {
                sock_conn_fd = accept4(sock_listen_fd, (struct sockaddr *)&client_addr, &socklen, SOCK_NONBLOCK);
                if (sock_conn_fd == -1)
                {
                    error("Error accepting new connection..\n");
                }

                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = sock_conn_fd;
                if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock_conn_fd, &ev) == -1)
                {
                    error("Error adding new event to epoll..\n");
                }
            }
            else
            {
                int newsockfd = events[i].data.fd;
                int bytes_received = recv(newsockfd, buffer, MAX_MESSAGE_LEN, 0);
                if (bytes_received <= 0)
                {
                    epoll_ctl(epollfd, EPOLL_CTL_DEL, newsockfd, NULL);
                    shutdown(newsockfd, SHUT_RDWR);
                }
                else
                {
                    send(newsockfd, buffer, bytes_received, 0);
                }
            }
        }
    }
}

