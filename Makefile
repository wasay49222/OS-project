CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O0 -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread

all: protocol_test

protocol_test: tests/protocol_test.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

check: protocol_test
	./protocol_test

clean:
	rm -f protocol_test switch host swctl

.PHONY: all check clean
