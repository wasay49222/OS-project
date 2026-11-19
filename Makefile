CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O0 -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread

all: switch host protocol_test

switch: src/switch.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

host: src/host.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

protocol_test: tests/protocol_test.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

check: protocol_test switch
	./protocol_test
	python3 tests/switch_integration.py

clean:
	rm -f protocol_test switch host swctl

.PHONY: all check clean
