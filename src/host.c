#define _POSIX_C_SOURCE 200809L

#include "frame.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stdbool.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PTHREAD_CHECK(call) pthread_check((call))

typedef struct {
    int fd;
    atomic_bool finished;
    pthread_mutex_t output_lock;
} Host;

static void pthread_check(int result)
{
    if (result != 0) {
        fprintf(stderr, "pthread call failed: %s\n", strerror(result));
        abort();
    }
}

static void close_fd(int fd)
{
    if (close(fd) < 0) {
        perror("close");
        abort();
    }
}

static int parse_mac(const char *text, uint8_t address[6])
{
    unsigned values[6];
    char extra;
    if (sscanf(text, "%2x:%2x:%2x:%2x:%2x:%2x%c", &values[0], &values[1],
               &values[2], &values[3], &values[4], &values[5], &extra) != 6) {
        return -1;
    }
    for (size_t i = 0; i < 6; i++) {
        address[i] = (uint8_t)values[i];
    }
    return 0;
}

static int print_mac(const uint8_t address[6])
{
    return printf("%02x:%02x:%02x:%02x:%02x:%02x", address[0], address[1],
                  address[2], address[3], address[4], address[5]) < 0 ? -1 : 0;
}

static void *receive_frames(void *argument)
{
    Host *host = argument;
    Frame frame;

    while (frame_read(host->fd, &frame) == 1) {
        PTHREAD_CHECK(pthread_mutex_lock(&host->output_lock));
        int failed = printf("received from ") < 0 || print_mac(frame.header + 2) < 0 ||
                     printf(" to ") < 0 || print_mac(frame.header + 8) < 0 ||
                     printf(" vlan %u priority %u payload: ", frame_vlan(&frame),
                            frame_priority(&frame)) < 0;
        if (!failed && frame.payload_length != 0 &&
            fwrite(frame.payload, 1, frame.payload_length, stdout) !=
                frame.payload_length) {
            failed = 1;
        }
        if (!failed && (putchar('\n') == EOF || fflush(stdout) == EOF)) {
            failed = 1;
        }
        PTHREAD_CHECK(pthread_mutex_unlock(&host->output_lock));
        if (failed) {
            perror("write host output");
            break;
        }
    }
    atomic_store(&host->finished, true);
    return NULL;
}

static int connect_to_switch(const char *ip, const char *port)
{
    struct addrinfo hints = {0};
    struct addrinfo *addresses = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(ip, port, &hints, &addresses) != 0) {
        return -1;
    }
    int fd = -1;
    for (struct addrinfo *address = addresses; address != NULL;
         address = address->ai_next) {
        fd = socket(address->ai_family, address->ai_socktype,
                    address->ai_protocol);
        if (fd >= 0 && connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
            break;
        }
        if (fd >= 0) {
            close_fd(fd);
            fd = -1;
        }
    }
    freeaddrinfo(addresses);
    return fd;
}

static int send_line(Host *host, const uint8_t source[6], char *line)
{
    char *save = NULL;
    char *destination_text = strtok_r(line, " \t\r\n", &save);
    char *vlan_text = strtok_r(NULL, " \t\r\n", &save);
    char *priority_text = strtok_r(NULL, " \t\r\n", &save);
    char *payload_text = strtok_r(NULL, "\r\n", &save);
    if (destination_text == NULL || vlan_text == NULL || priority_text == NULL) {
        return -1;
    }

    uint8_t destination[6];
    if (parse_mac(destination_text, destination) < 0) {
        return -1;
    }
    char *end = NULL;
    unsigned long vlan = strtoul(vlan_text, &end, 10);
    if (*vlan_text == '\0' || *end != '\0' || vlan < 1 || vlan > 4094) {
        return -1;
    }
    unsigned long priority = strtoul(priority_text, &end, 10);
    if (*priority_text == '\0' || *end != '\0' || priority > 2) {
        return -1;
    }
    size_t payload_length = payload_text == NULL ? 0 : strlen(payload_text);
    if (payload_length > FRAME_MAX_PAYLOAD) {
        return -1;
    }

    Frame frame = {0};
    uint16_t wire_value = htons((uint16_t)payload_length);
    memcpy(frame.header, &wire_value, sizeof(wire_value));
    memcpy(frame.header + 2, source, 6);
    memcpy(frame.header + 8, destination, 6);
    wire_value = htons((uint16_t)vlan);
    memcpy(frame.header + 14, &wire_value, sizeof(wire_value));
    frame.header[16] = (uint8_t)priority;
    frame.payload_length = (uint16_t)payload_length;
    if (payload_length != 0) {
        memcpy(frame.payload, payload_text, payload_length);
    }
    return frame_write(host->fd, &frame);
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stderr, "usage: %s <switch_ip> <tcp_port> <mac>\n", argv[0]);
        return EXIT_FAILURE;
    }
    uint8_t source[6];
    if (parse_mac(argv[3], source) < 0) {
        fprintf(stderr, "invalid MAC address\n");
        return EXIT_FAILURE;
    }
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("signal SIGPIPE");
        return EXIT_FAILURE;
    }
    Host host = {0};
    host.fd = connect_to_switch(argv[1], argv[2]);
    if (host.fd < 0) {
        fprintf(stderr, "could not connect to switch\n");
        return EXIT_FAILURE;
    }
    if (pthread_mutex_init(&host.output_lock, NULL) != 0) {
        close_fd(host.fd);
        return EXIT_FAILURE;
    }
    atomic_init(&host.finished, false);
    pthread_t receiver;
    if (pthread_create(&receiver, NULL, receive_frames, &host) != 0) {
        PTHREAD_CHECK(pthread_mutex_destroy(&host.output_lock));
        close_fd(host.fd);
        return EXIT_FAILURE;
    }

    char *line = NULL;
    size_t capacity = 0;
    while (!atomic_load(&host.finished) && getline(&line, &capacity, stdin) >= 0) {
        if (send_line(&host, source, line) < 0) {
            fprintf(stderr, "invalid frame or connection closed\n");
        }
    }
    if (ferror(stdin)) {
        perror("read host input");
    }
    free(line);
    if (shutdown(host.fd, SHUT_RDWR) < 0 && errno != ENOTCONN) {
        perror("shutdown");
    }
    PTHREAD_CHECK(pthread_join(receiver, NULL));
    close_fd(host.fd);
    PTHREAD_CHECK(pthread_mutex_destroy(&host.output_lock));
    return EXIT_SUCCESS;
}
