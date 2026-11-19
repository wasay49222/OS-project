# Virtual Switch

Operating Systems Fall 2026 course project.

## Build

Run `make` to build the switch, test host, and protocol test program. Run `make check` to run the protocol and forwarding checks. Run `make clean` to remove generated files.

## Run

The switch will be started as `./switch <tcp_port> <num_ports>`. The test host will be started as `./host <switch_ip> <tcp_port> <mac>`. Enter frames at the host prompt as `destination_mac vlan priority payload`, for example `02:00:00:00:00:0b 10 1 hello`. Received frames are printed to the terminal.

## Configuration

Tier 1 does not use a configuration file. Its current limits are 64 queued frames per port, 1,024 MAC table entries, 16 learned MAC addresses per port, a 300-second MAC age, and a five-second wait for a free port.

## Tier 1 status

Tier 1 items 1 to 5 are implemented. Frames use the fixed 17-byte header, and the switch handles fragmented input, partial writes, truncated connections, broadcasts, unknown destinations, and learned unicast. Each port has a bounded output queue and sender thread. Broadcast packets share one frame with reference counting. The MAC table uses one lock per bucket, aging, least-recently-used eviction, move logging, and a per-port MAC limit. A timed semaphore limits active hosts.

## Checks

Run `make check` to build with the course flags and test frame handling, forwarding, broadcast, slow-host isolation, disconnect recovery, the port limit, MAC moves, MAC aging, LRU eviction, port security, and clean SIGINT shutdown. Protocol, forwarding, lifecycle, table, and security tests were also run under Valgrind and ThreadSanitizer; the slow-host stress was run under ThreadSanitizer.

One real bug found during development was a client socket being closed twice when port reservation failed. GCC's static analyzer found it; the server now leaves the socket close to its owning caller.
