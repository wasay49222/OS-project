#include "frame.h"
#include "io.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void test_fragmented_frame(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    uint8_t wire[FRAME_HEADER_SIZE + 3] = {0};
    uint16_t length = htons(3);
    uint16_t vlan = htons(10);
    memcpy(wire, &length, sizeof(length));
    memcpy(wire + 14, &vlan, sizeof(vlan));
    wire[16] = 2;
    wire[FRAME_HEADER_SIZE] = 'o';
    wire[FRAME_HEADER_SIZE + 1] = 's';
    wire[FRAME_HEADER_SIZE + 2] = '!';

    for (size_t i = 0; i < sizeof(wire); i++) {
        assert(write_full(sockets[0], wire + i, 1) == 0);
    }

    Frame frame;
    assert(frame_read(sockets[1], &frame) == 1);
    assert(frame.payload_length == 3);
    assert(frame_vlan(&frame) == 10);
    assert(frame_priority(&frame) == 2);
    assert(memcmp(frame.payload, "os!", 3) == 0);
    close(sockets[0]);
    close(sockets[1]);
}

static void test_invalid_length(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    uint8_t header[FRAME_HEADER_SIZE] = {0};
    uint16_t length = htons(FRAME_MAX_PAYLOAD + 1U);
    uint16_t vlan = htons(10);
    memcpy(header, &length, sizeof(length));
    memcpy(header + 14, &vlan, sizeof(vlan));
    assert(write_full(sockets[0], header, sizeof(header)) == 0);

    Frame frame;
    assert(frame_read(sockets[1], &frame) == -1);
    close(sockets[0]);
    close(sockets[1]);
}

static void test_invalid_vlan_and_priority(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    uint8_t header[FRAME_HEADER_SIZE] = {0};
    uint16_t vlan = htons(10);
    memcpy(header + 14, &vlan, sizeof(vlan));
    header[16] = 3;
    assert(write_full(sockets[0], header, sizeof(header)) == 0);
    Frame frame;
    assert(frame_read(sockets[1], &frame) == -1);
    close(sockets[0]);
    close(sockets[1]);

    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    memset(header, 0, sizeof(header));
    header[16] = 0;
    assert(write_full(sockets[0], header, sizeof(header)) == 0);
    assert(frame_read(sockets[1], &frame) == -1);
    close(sockets[0]);
    close(sockets[1]);
}

static void test_partial_eof(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    uint8_t partial[4] = {0};
    assert(write_full(sockets[0], partial, sizeof(partial)) == 0);
    close(sockets[0]);

    Frame frame;
    assert(frame_read(sockets[1], &frame) == -2);
    close(sockets[1]);
}

static void test_truncated_payload(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    uint8_t wire[FRAME_HEADER_SIZE + 1] = {0};
    uint16_t length = htons(3);
    uint16_t vlan = htons(10);
    memcpy(wire, &length, sizeof(length));
    memcpy(wire + 14, &vlan, sizeof(vlan));
    wire[FRAME_HEADER_SIZE] = 'x';
    assert(write_full(sockets[0], wire, sizeof(wire)) == 0);
    close(sockets[0]);
    Frame frame;
    assert(frame_read(sockets[1], &frame) == -1);
    close(sockets[1]);
}

typedef struct {
    int fd;
    uint8_t *buffer;
    size_t length;
} DrainTask;

static void *drain_socket(void *argument)
{
    DrainTask *task = argument;
    assert(read_full(task->fd, task->buffer, task->length) == 1);
    return NULL;
}

static void test_partial_write(void)
{
    int sockets[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    int buffer_size = 1024;
    assert(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &buffer_size,
                      sizeof(buffer_size)) == 0);
    size_t length = 1024U * 1024U;
    uint8_t *sent = malloc(length);
    uint8_t *received = malloc(length);
    assert(sent != NULL && received != NULL);
    for (size_t i = 0; i < length; i++) {
        sent[i] = (uint8_t)(i % 251U);
    }
    DrainTask task = {sockets[1], received, length};
    pthread_t reader;
    assert(pthread_create(&reader, NULL, drain_socket, &task) == 0);
    assert(write_full(sockets[0], sent, length) == 0);
    assert(pthread_join(reader, NULL) == 0);
    assert(memcmp(sent, received, length) == 0);
    free(received);
    free(sent);
    close(sockets[0]);
    close(sockets[1]);
}

int main(void)
{
    test_fragmented_frame();
    test_invalid_length();
    test_invalid_vlan_and_priority();
    test_partial_eof();
    test_truncated_payload();
    test_partial_write();
    return 0;
}
