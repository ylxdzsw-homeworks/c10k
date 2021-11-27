epoll: epoll.c
	cc -O3 -static -lrt -pthread -o epoll epoll.c
