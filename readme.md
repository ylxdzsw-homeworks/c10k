related parameters:
sysctl -w fs.file-max=1073741816
sysctl -w fs.nr_open=1073741816
sysctl -w fs.epoll.max_user_watches=10000000

/etc/security/limits.conf
ulimit -n $(ulimit -Hn)

