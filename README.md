# Virtual Switch

Operating Systems Fall 2026 course project.

## Build

Run `make check` to build and run the protocol tests. Run `make clean` to remove generated files. The switch and host executables are not implemented yet.

## Run

The switch will be started as `./switch <tcp_port> <num_ports>`. The test host will be started as `./host <switch_ip> <tcp_port> <mac>`.

## Configuration

Configuration file format and optional settings will be documented as they are implemented.

## Project status

Implementation is in progress. The frame reader and writer handle fragmented TCP data, network byte order, oversized payload lengths, and truncated input. Further completed requirements and test results will be recorded here as features are added.
