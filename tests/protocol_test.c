#include "frame.h"
#include "io.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdint.h>
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
    memcpy(header, &length, sizeof(length));
    assert(write_full(sockets[0], header, sizeof(header)) == 0);

    Frame frame;
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

    uint8_t buffer[FRAME_HEADER_SIZE];
    assert(read_full(sockets[1], buffer, sizeof(buffer)) == -2);
    close(sockets[1]);
}

int main(void)
{
    test_fragmented_frame();
    test_invalid_length();
    test_partial_eof();
    return 0;
}
