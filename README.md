# Virtual Switch

Operating Systems Fall 2026 course project.

## Build

Run `make` to build the switch, test host, and protocol test program. Run `make check` to run the protocol and forwarding checks. Run `make clean` to remove generated files.

## Run

The switch will be started as `./switch <tcp_port> <num_ports>`. The test host will be started as `./host <switch_ip> <tcp_port> <mac>`. Enter frames at the host prompt as `destination_mac vlan priority payload`, for example `02:00:00:00:00:0b 10 1 hello`. Received frames are printed to the terminal.

## Configuration

Configuration file format and optional settings will be documented as they are implemented.

## Project status

Implementation is in progress. The frame reader and writer handle fragmented TCP data, network byte order, oversized payload lengths, and truncated input. The switch currently learns source MAC addresses, floods unknown and broadcast traffic, forwards known unicast traffic, and uses a bounded queue and sender thread per port. MAC table aging, size limiting, and per-port MAC limits are implemented. Further completed requirements and test results will be recorded here as features are added.
