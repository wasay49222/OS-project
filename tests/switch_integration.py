import os
import shlex
import socket
import struct
import subprocess
import time
import select

SWITCH_PORT = 23147
MAC_A = bytes.fromhex("02000000000a")
MAC_B = bytes.fromhex("02000000000b")
MAC_C = bytes.fromhex("02000000000c")
BROADCAST = b"\xff" * 6


def connect_when_ready(process):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("switch exited before accepting clients")
        try:
            client = socket.create_connection(("127.0.0.1", SWITCH_PORT), timeout=1)
            client.settimeout(1)
            return client
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("switch did not start listening")


def wait_for_connected(process, count):
    connected = 0
    events = []
    pending = b""
    deadline = time.monotonic() + 3
    while connected < count and time.monotonic() < deadline:
        ready, _, _ = select.select([process.stderr], [], [], 0.1)
        if ready:
            chunk = os.read(process.stderr.fileno(), 4096)
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                events.append(line.decode(errors="replace"))
                if b"host connected on port" in line:
                    connected += 1
    if connected != count:
        raise RuntimeError(f"switch activated {connected}/{count} ports: {events}")


def send_frame(client, source, destination, payload):
    header = struct.pack("!H", len(payload)) + source + destination + struct.pack("!HB", 10, 1)
    wire = header + payload
    for byte in wire:
        client.sendall(bytes((byte,)))


def receive_frame(client):
    header = bytearray()
    while len(header) < 17:
        part = client.recv(17 - len(header))
        if not part:
            raise RuntimeError("connection closed before frame header arrived")
        header.extend(part)
    length = struct.unpack("!H", header[:2])[0]
    payload = bytearray()
    while len(payload) < length:
        part = client.recv(length - len(payload))
        if not part:
            raise RuntimeError("connection closed before payload arrived")
        payload.extend(part)
    return bytes(header) + bytes(payload)


def main():
    command = shlex.split(os.environ.get("SWITCH_BINARY", "./switch"))
    command.extend([str(SWITCH_PORT), "3"])
    process = subprocess.Popen(command,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
    clients = []
    try:
        a = connect_when_ready(process)
        clients.append(a)
        b = socket.create_connection(("127.0.0.1", SWITCH_PORT), timeout=1)
        b.settimeout(1)
        clients.append(b)
        c = socket.create_connection(("127.0.0.1", SWITCH_PORT), timeout=1)
        c.settimeout(1)
        clients.append(c)
        wait_for_connected(process, 3)

        send_frame(a, MAC_A, MAC_B, b"learn A")
        assert receive_frame(b).endswith(b"learn A")
        assert receive_frame(c).endswith(b"learn A")

        send_frame(b, MAC_B, MAC_A, b"learn B")
        assert receive_frame(a).endswith(b"learn B")
        c.settimeout(0.2)
        try:
            receive_frame(c)
            raise AssertionError("known unicast was sent to an unrelated port")
        except socket.timeout:
            pass

        send_frame(a, MAC_A, MAC_B, b"known unicast")
        assert receive_frame(b).endswith(b"known unicast")
        try:
            receive_frame(c)
            raise AssertionError("known unicast was flooded")
        except socket.timeout:
            pass

        c.settimeout(1)
        send_frame(a, MAC_A, BROADCAST, b"broadcast")
        assert receive_frame(b).endswith(b"broadcast")
        assert receive_frame(c).endswith(b"broadcast")
    finally:
        for client in clients:
            client.close()
        process.send_signal(2)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        if process.returncode != 0:
            raise RuntimeError(process.stderr.read().decode(errors="replace"))


if __name__ == "__main__":
    main()
