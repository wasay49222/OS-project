CC = gcc
CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O0 -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread

all: switch host swctl protocol_test

switch: src/switch.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

host: src/host.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

swctl: src/swctl.c src/stats.h
	$(CC) $(CFLAGS) -Isrc src/swctl.c $(LDFLAGS) -o $@

switch_smallmac: src/switch.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -DMAC_LIMIT=4 -DMACS_PER_PORT=100 -Isrc $^ $(LDFLAGS) -o $@

switch_fastaging: src/switch.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -DMAC_AGE_SECONDS=1 -DMACS_PER_PORT=100 -Isrc $^ $(LDFLAGS) -o $@

protocol_test: tests/protocol_test.c src/frame.c src/io.c
	$(CC) $(CFLAGS) -Isrc $^ $(LDFLAGS) -o $@

check: protocol_test switch swctl switch_smallmac switch_fastaging
	./protocol_test
	python3 tests/switch_integration.py
	python3 tests/tier1_edge_tests.py
	python3 tests/tier2_network_tests.py

clean:
	rm -f protocol_test switch switch_smallmac switch_fastaging host swctl

.PHONY: all check clean
