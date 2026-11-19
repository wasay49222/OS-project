import os
import select
import shlex
import socket
import subprocess
import time
import threading

BASE_PORT = 23160


def start_switch(port, ports, binary="./switch"):
    binary = os.environ.get("SWITCH_BINARY", binary)
    process = subprocess.Popen(shlex.split(binary) + [str(port), str(ports)],
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    process.connected_events = 0
    process.event_buffer = b""
    return process


def connect_client(process, port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("switch exited before accepting clients")
        try:
            client = socket.create_connection(("127.0.0.1", port), timeout=1)
            client.settimeout(2)
            return client
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("switch did not start listening")


def wait_for_connections(process, count):
    connected = process.connected_events
    events = []
    deadline = time.monotonic() + 5
    while connected < count and time.monotonic() < deadline:
        ready, _, _ = select.select([process.stderr], [], [], 0.1)
        if ready:
            process.event_buffer += os.read(process.stderr.fileno(), 4096)
            while b"\n" in process.event_buffer:
                line, process.event_buffer = process.event_buffer.split(b"\n", 1)
                events.append(line.decode(errors="replace"))
                if b"host connected on port" in line:
                    connected += 1
                    process.connected_events = connected
    if connected != count:
        raise RuntimeError(f"connected {connected}/{count}: {events}")


def stop_switch(process, clients):
    for client in clients:
        client.close()
    if process.poll() is None:
        process.send_signal(2)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise
    output = process.stderr.read().decode(errors="replace")
    if process.returncode != 0:
        raise RuntimeError(output)


def make_frame(source, destination, payload):
    return (len(payload).to_bytes(2, "big") + source + destination +
            (10).to_bytes(2, "big") + b"\x01" + payload)


def send_frame(client, source, destination, payload):
    client.sendall(make_frame(source, destination, payload))


def receive_frame(client):
    parts = bytearray()
    while len(parts) < 17:
        data = client.recv(17 - len(parts))
        if not data:
            raise RuntimeError("connection closed before frame header")
        parts.extend(data)
    size = int.from_bytes(parts[:2], "big")
    while len(parts) < 17 + size:
        data = client.recv(17 + size - len(parts))
        if not data:
            raise RuntimeError("connection closed before frame payload")
        parts.extend(data)
    return bytes(parts)


def test_partial_disconnect_and_reuse():
    port = BASE_PORT
    process = start_switch(port, 2)
    clients = []
    try:
        a = connect_client(process, port)
        b = connect_client(process, port)
        clients.extend([a, b])
        wait_for_connections(process, 2)
        a.sendall(b"\x00\x05\x02\x00")
        a.close()
        replacement = connect_client(process, port)
        clients.append(replacement)
        wait_for_connections(process, 3)
        send_frame(b, bytes.fromhex("02000000000b"),
                   bytes.fromhex("02000000000c"), b"after disconnect")
        assert receive_frame(replacement).endswith(b"after disconnect")
    finally:
        stop_switch(process, clients)


def test_slow_host_does_not_block_fast_host():
    port = BASE_PORT + 1
    process = start_switch(port, 3)
    clients = []
    try:
        source = connect_client(process, port)
        fast = connect_client(process, port)
        slow = connect_client(process, port)
        fast.settimeout(10)
        clients.extend([source, fast, slow])
        wait_for_connections(process, 3)
        marker_received = threading.Event()
        errors = []

        def read_fast_host():
            try:
                while not marker_received.is_set():
                    frame = receive_frame(fast)
                    if frame.endswith(b"fast-path-marker"):
                        marker_received.set()
            except Exception as error:
                errors.append(error)

        reader = threading.Thread(target=read_fast_host)
        reader.start()
        src = bytes.fromhex("020000000021")
        dst = b"\xff" * 6
        payload = b"x" * 1400
        wire = make_frame(src, dst, payload)
        for _ in range(8192):
            source.sendall(wire)
        send_frame(source, src, dst, b"fast-path-marker")
        if not marker_received.wait(10):
            raise AssertionError(f"fast host stopped receiving: {errors}")
        reader.join(timeout=2)
        assert not errors
    finally:
        stop_switch(process, clients)


def test_switch_full_reply():
    port = BASE_PORT + 2
    process = start_switch(port, 1)
    clients = []
    try:
        first = connect_client(process, port)
        clients.append(first)
        wait_for_connections(process, 1)
        second = socket.create_connection(("127.0.0.1", port), timeout=1)
        second.settimeout(7)
        clients.append(second)
        assert second.recv(12) == b"switch full\n"
    finally:
        stop_switch(process, clients)


def test_mac_move_updates_forwarding():
    port = BASE_PORT + 3
    process = start_switch(port, 3)
    clients = []
    try:
        a = connect_client(process, port)
        b = connect_client(process, port)
        c = connect_client(process, port)
        clients.extend([a, b, c])
        wait_for_connections(process, 3)
        mac_a = bytes.fromhex("020000000031")
        mac_b = bytes.fromhex("020000000032")
        mac_c = bytes.fromhex("020000000033")
        broadcast = b"\xff" * 6
        send_frame(a, mac_a, broadcast, b"initial A")
        assert receive_frame(b).endswith(b"initial A")
        assert receive_frame(c).endswith(b"initial A")
        send_frame(b, mac_a, broadcast, b"A moved to B")
        assert receive_frame(a).endswith(b"A moved to B")
        assert receive_frame(c).endswith(b"A moved to B")
        send_frame(c, mac_c, mac_a, b"known after move")
        assert receive_frame(b).endswith(b"known after move")
        a.settimeout(0.2)
        try:
            receive_frame(a)
            raise AssertionError("moved MAC still forwarded to old port")
        except socket.timeout:
            pass
    finally:
        stop_switch(process, clients)


def test_port_security_disconnects_excess_macs():
    port = BASE_PORT + 4
    process = start_switch(port, 2)
    clients = []
    try:
        source = connect_client(process, port)
        receiver = connect_client(process, port)
        clients.extend([source, receiver])
        wait_for_connections(process, 2)
        broadcast = b"\xff" * 6
        for index in range(17):
            mac = bytes.fromhex("0200000000") + bytes((index,))
            send_frame(source, mac, broadcast, b"security")
        source.settimeout(3)
        assert source.recv(1) == b""
        replacement = connect_client(process, port)
        clients.append(replacement)
        wait_for_connections(process, 3)
    finally:
        stop_switch(process, clients)


def test_lru_table_eviction():
    port = BASE_PORT + 5
    process = start_switch(port, 3, "./switch_smallmac")
    clients = []
    try:
        a = connect_client(process, port)
        b = connect_client(process, port)
        c = connect_client(process, port)
        clients.extend([a, b, c])
        wait_for_connections(process, 3)
        macs = [bytes.fromhex(f"0200000000{index:02x}") for index in range(41, 46)]
        broadcast = b"\xff" * 6
        for index in range(3):
            send_frame(a, macs[index], broadcast, b"learn")
            assert receive_frame(b).endswith(b"learn")
            assert receive_frame(c).endswith(b"learn")
        send_frame(b, macs[3], broadcast, b"fourth")
        assert receive_frame(a).endswith(b"fourth")
        assert receive_frame(c).endswith(b"fourth")
        send_frame(b, macs[3], macs[0], b"touch oldest")
        assert receive_frame(a).endswith(b"touch oldest")
        send_frame(c, macs[4], broadcast, b"evict least recent")
        assert receive_frame(a).endswith(b"evict least recent")
        assert receive_frame(b).endswith(b"evict least recent")
        send_frame(c, macs[4], macs[1], b"evicted entry")
        assert receive_frame(a).endswith(b"evicted entry")
        assert receive_frame(b).endswith(b"evicted entry")
        send_frame(c, macs[4], macs[0], b"retained entry")
        assert receive_frame(a).endswith(b"retained entry")
        b.settimeout(0.2)
        try:
            receive_frame(b)
            raise AssertionError("LRU eviction removed a recently used entry")
        except socket.timeout:
            pass
    finally:
        stop_switch(process, clients)


def test_mac_aging_forgets_old_entries():
    port = BASE_PORT + 6
    process = start_switch(port, 3, "./switch_fastaging")
    clients = []
    try:
        a = connect_client(process, port)
        b = connect_client(process, port)
        c = connect_client(process, port)
        clients.extend([a, b, c])
        wait_for_connections(process, 3)
        mac_a = bytes.fromhex("020000000051")
        mac_b = bytes.fromhex("020000000052")
        broadcast = b"\xff" * 6
        send_frame(a, mac_a, broadcast, b"learn A")
        assert receive_frame(b).endswith(b"learn A")
        assert receive_frame(c).endswith(b"learn A")
        send_frame(b, mac_b, mac_a, b"known before age")
        assert receive_frame(a).endswith(b"known before age")
        c.settimeout(0.2)
        try:
            receive_frame(c)
            raise AssertionError("known destination was flooded before aging")
        except socket.timeout:
            pass
        time.sleep(2.2)
        send_frame(b, mac_b, mac_a, b"unknown after age")
        assert receive_frame(a).endswith(b"unknown after age")
        assert receive_frame(c).endswith(b"unknown after age")
    finally:
        stop_switch(process, clients)


def test_sigint_shuts_down_active_ports():
    port = BASE_PORT + 7
    process = start_switch(port, 2)
    clients = []
    try:
        clients.append(connect_client(process, port))
        clients.append(connect_client(process, port))
        wait_for_connections(process, 2)
        process.send_signal(2)
        for client in clients:
            client.settimeout(3)
            assert client.recv(1) == b""
        assert process.wait(timeout=5) == 0
        output = process.stderr.read().decode(errors="replace")
        if output and "failed" in output:
            raise AssertionError(output)
    finally:
        for client in clients:
            client.close()
        if process.poll() is None:
            stop_switch(process, [])


def main():
    test_partial_disconnect_and_reuse()
    test_slow_host_does_not_block_fast_host()
    test_switch_full_reply()
    test_mac_move_updates_forwarding()
    test_port_security_disconnects_excess_macs()
    test_lru_table_eviction()
    test_mac_aging_forgets_old_entries()
    test_sigint_shuts_down_active_ports()


if __name__ == "__main__":
    main()
