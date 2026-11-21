# Virtual Switch

Operating Systems Fall 2026 course project.

## Build

Run `make` to build the switch, test host, `swctl` admin tool, and protocol test program. Run `make check` to run the protocol and integration checks. Run `make clean` to remove generated files.

## Run

Start the switch with `./switch <tcp_port> <num_ports> [config_file]`. If no file is given, the switch reads `switch.conf` from its current directory. Start the test host with `./host <switch_ip> <tcp_port> <mac>`. Enter frames at the host prompt as `destination_mac vlan priority payload`, for example `02:00:00:00:00:0b 10 1 hello`. Received frames are printed to the terminal.

## Configuration

Each config line sets one option: `port N vlan V`, `port N trunk`, `port N rate X`, or `mirror SOURCE SNIFFER`. Port numbers start at 1. VLAN values are 1 to 4094. A rate of 0 means unlimited; other rates are frames per second. For example:

```text
port 1 vlan 10
port 2 vlan 20
port 3 trunk
port 1 rate 100
mirror 1 4
```

The switch supports 64 queued frames per port, 1,024 MAC table entries, 16 learned MAC addresses per port, a 300-second MAC age, and a five-second wait for a free port. Each port has high, normal, and low priority queues; the sender uses a 5:3:1 schedule and moves an aged low-priority frame forward. A timer refills rate-limit tokens every 100 ms.

## Admin commands

Run `./swctl show mac`, `./swctl show ports`, `./swctl stats`, `./swctl stats --watch`, `./swctl shutdown PORT`, `./swctl up PORT`, `./swctl vlan PORT VLAN`, `./swctl mirror PORT SNIFFER`, or `./swctl clear mac`. The switch creates a local UNIX socket at `/tmp/virtual_switch_admin.sock` and shared counters at `/virtual_switch_stats`. `SIGHUP` reloads the config file, `SIGUSR1` writes `mac_table.dump`, and `SIGINT` stops the switch. Timestamped messages are written by a logger thread to `switch.log`.

The main lock order is the port table lock before a port queue or rate lock, then shared statistics. MAC eviction takes the eviction lock before one MAC bucket lock. MAC bucket locks are not nested.

## Tier 1 status

Tier 1 items 1 to 5 and Tier 2 items 6 to 13 are implemented. Frames use the fixed 17-byte header, and the switch handles fragmented input, partial writes, truncated connections, broadcasts, unknown destinations, and learned unicast. Each port has bounded priority queues and a sender thread. Broadcast packets share one frame with reference counting. The MAC table uses one lock per bucket, aging, least-recently-used eviction, move logging, and a per-port MAC limit. A timed semaphore limits active hosts. Tier 2 adds weighted scheduling, aging, token-bucket limits, VLANs and trunk ports, port mirroring, live admin commands, process-shared counters, signal handling, and queued timestamped logging.

## Checks

Run `make check` to build with the course flags and test frame handling, forwarding, broadcast, slow-host isolation, disconnect recovery, the port limit, MAC moves, MAC aging, LRU eviction, port security, clean SIGINT shutdown, VLAN isolation and trunking, mirroring, config reload, rate limiting, admin commands, shared counters, and log output. Protocol, forwarding, lifecycle, table, and security tests were also run under Valgrind and ThreadSanitizer; the slow-host stress was run under ThreadSanitizer.

One real bug found during development was a client socket being closed twice when port reservation failed. GCC's static analyzer found it; the server now leaves the socket close to its owning caller.
