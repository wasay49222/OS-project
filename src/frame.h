#ifndef FRAME_H
#define FRAME_H

#include <stdint.h>

#define FRAME_HEADER_SIZE 17U
#define FRAME_MAX_PAYLOAD 1500U
#define FRAME_BROADCAST_MAC "\xff\xff\xff\xff\xff\xff"

typedef struct {
    uint8_t header[FRAME_HEADER_SIZE];
    uint8_t payload[FRAME_MAX_PAYLOAD];
    uint16_t payload_length;
} Frame;

int frame_read(int fd, Frame *frame);
int frame_write(int fd, const Frame *frame);
uint16_t frame_vlan(const Frame *frame);
uint8_t frame_priority(const Frame *frame);

#endif
