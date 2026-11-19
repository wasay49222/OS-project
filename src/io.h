#ifndef IO_H
#define IO_H

#include <stddef.h>
#include <sys/types.h>

int read_full(int fd, void *buffer, size_t length);
int write_full(int fd, const void *buffer, size_t length);

#endif
