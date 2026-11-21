#define _POSIX_C_SOURCE 200809L

#include "stats.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

static int send_command(const char *command)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    struct sockaddr_un address = {0};
    address.sun_family = AF_UNIX;
    memcpy(address.sun_path, SWITCH_ADMIN_PATH, sizeof(SWITCH_ADMIN_PATH));
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("connect switch admin socket");
        close(fd);
        return -1;
    }
    size_t length = strlen(command) + 1U;
    char request[129];
    if (length > sizeof(request)) {
        close(fd);
        errno = EMSGSIZE;
        perror("write command");
        return -1;
    }
    memcpy(request, command, length - 1U);
    request[length - 1U] = '\n';
    size_t sent = 0;
    while (sent < length) {
        ssize_t count = write(fd, request + sent, length - sent);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            perror("write command");
            close(fd);
            return -1;
        }
        sent += (size_t)count;
    }
    char buffer[512];
    int status = 0;
    for (;;) {
        ssize_t count = read(fd, buffer, sizeof(buffer));
        if (count == 0) {
            break;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("read response");
            status = -1;
            break;
        }
        if (fwrite(buffer, 1, (size_t)count, stdout) != (size_t)count) {
            perror("write output");
            status = -1;
            break;
        }
    }
    close(fd);
    return status;
}

static int print_stats(const SharedStats *stats)
{
    if (pthread_mutex_lock((pthread_mutex_t *)&stats->lock) != 0) {
        fprintf(stderr, "could not lock shared statistics\n");
        return -1;
    }
    printf("%-6s %-12s %-12s %-12s %-12s\n", "port", "in", "out", "dropped", "mirrored");
    for (unsigned i = 0; i < stats->port_count; i++) {
        const PortStats *port = &stats->ports[i];
        printf("%-6u %-12llu %-12llu %-12llu %-12llu\n", i + 1U,
               (unsigned long long)port->in, (unsigned long long)port->out,
               (unsigned long long)port->dropped,
               (unsigned long long)port->mirrored);
    }
    pthread_mutex_unlock((pthread_mutex_t *)&stats->lock);
    return 0;
}

static int watch_stats(void)
{
    int fd = shm_open(SWITCH_STATS_NAME, O_RDWR, 0);
    if (fd < 0) {
        perror("open shared statistics (is the switch running?)");
        return -1;
    }
    struct stat info;
    if (fstat(fd, &info) < 0 || info.st_size != (off_t)sizeof(SharedStats)) {
        perror("check shared statistics");
        close(fd);
        return -1;
    }
    SharedStats *stats = mmap(NULL, sizeof(*stats), PROT_READ | PROT_WRITE,
                              MAP_SHARED, fd, 0);
    close(fd);
    if (stats == MAP_FAILED) {
        perror("map shared statistics");
        return -1;
    }
    while (1) {
        if (printf("\033[H\033[J") < 0 || print_stats(stats) < 0 || fflush(stdout) == EOF) {
            munmap(stats, sizeof(*stats));
            return -1;
        }
        struct timespec delay = {.tv_sec = 1, .tv_nsec = 0};
        while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {
        }
    }
}

static void usage(const char *program)
{
    fprintf(stderr, "usage: %s show mac|show ports|shutdown PORT|up PORT|vlan PORT VLAN|mirror PORT SNIFFER|clear mac|stats [--watch]\n", program);
}

int main(int argc, char **argv)
{
    if (argc == 3 && strcmp(argv[1], "stats") == 0 &&
        strcmp(argv[2], "--watch") == 0) {
        return watch_stats() == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }
    if ((argc == 3 && ((strcmp(argv[1], "show") == 0 &&
                        (strcmp(argv[2], "mac") == 0 || strcmp(argv[2], "ports") == 0)) ||
                       (strcmp(argv[1], "clear") == 0 && strcmp(argv[2], "mac") == 0))) ||
        (argc == 2 && strcmp(argv[1], "stats") == 0) ||
        (argc == 3 && (strcmp(argv[1], "shutdown") == 0 ||
                       strcmp(argv[1], "up") == 0)) ||
        (argc == 4 && (strcmp(argv[1], "vlan") == 0 ||
                       strcmp(argv[1], "mirror") == 0))) {
        char command[128];
        int length;
        if (argc == 2) {
            length = snprintf(command, sizeof(command), "%s", argv[1]);
        } else if (argc == 3) {
            length = snprintf(command, sizeof(command), "%s %s", argv[1], argv[2]);
        } else {
            length = snprintf(command, sizeof(command), "%s %s %s", argv[1], argv[2], argv[3]);
        }
        if (length < 0 || (size_t)length >= sizeof(command)) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
        return send_command(command) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
    }
    usage(argv[0]);
    return EXIT_FAILURE;
}
