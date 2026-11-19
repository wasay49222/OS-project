#include "frame.h"

#include "io.h"

#include <arpa/inet.h>
#include <stddef.h>
#include <string.h>

int frame_read(int fd, Frame *frame)
{
    int result = read_full(fd, frame->header, FRAME_HEADER_SIZE);
    if (result != 1) {
        return result;
    }

    uint16_t wire_length;
    memcpy(&wire_length, frame->header, sizeof(wire_length));
    frame->payload_length = ntohs(wire_length);
    if (frame->payload_length > FRAME_MAX_PAYLOAD || frame_priority(frame) > 2 ||
        frame_vlan(frame) < 1 || frame_vlan(frame) > 4094) {
        return -1;
    }

    if (frame->payload_length == 0) {
        return 1;
    }
    return read_full(fd, frame->payload, frame->payload_length) == 1 ? 1 : -1;
}

int frame_write(int fd, const Frame *frame)
{
    if (frame->payload_length > FRAME_MAX_PAYLOAD || frame_priority(frame) > 2 ||
        frame_vlan(frame) < 1 || frame_vlan(frame) > 4094) {
        return -1;
    }
    if (write_full(fd, frame->header, FRAME_HEADER_SIZE) < 0) {
        return -1;
    }
    return write_full(fd, frame->payload, frame->payload_length);
}

uint16_t frame_vlan(const Frame *frame)
{
    uint16_t wire_vlan;
    memcpy(&wire_vlan, frame->header + 14, sizeof(wire_vlan));
    return ntohs(wire_vlan);
}

uint8_t frame_priority(const Frame *frame)
{
    return frame->header[16];
}
