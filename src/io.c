#include "io.h"

#include <errno.h>
#include <unistd.h>

int read_full(int fd, void *buffer, size_t length)
{
    size_t received = 0;

    while (received < length) {
        ssize_t count = read(fd, (char *)buffer + received, length - received);
        if (count == 0) {
            return received == 0 ? 0 : -2;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        received += (size_t)count;
    }

    return 1;
}

int write_full(int fd, const void *buffer, size_t length)
{
    size_t sent = 0;

    while (sent < length) {
        ssize_t count = write(fd, (const char *)buffer + sent, length - sent);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (count == 0) {
            return -1;
        }
        sent += (size_t)count;
    }

    return 0;
}
