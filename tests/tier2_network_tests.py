import os
import re
import select
import socket
import subprocess
import tempfile
import time

BASE_PORT = 23180
SWITCH_BINARY = os.environ.get("SWITCH_BINARY", "./switch")


def receive_exact(client, length):
    result = bytearray()
    while len(result) < length:
        data = client.recv(length - len(result))
        if not data:
            raise RuntimeError("connection closed during frame")
        result.extend(data)
    return bytes(result)


def receive_frame(client):
    header = receive_exact(client, 17)
    return header + receive_exact(client, int.from_bytes(header[:2], "big"))


def send_frame(client, source, destination, vlan, priority, payload):
    wire = (len(payload).to_bytes(2, "big") + source + destination +
            vlan.to_bytes(2, "big") + bytes((priority,)) + payload)
    client.sendall(wire)


def connect_client(port):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            client = socket.create_connection(("127.0.0.1", port), timeout=1)
            client.settimeout(2)
            return client
        except ConnectionRefusedError:
            time.sleep(0.05)
    raise RuntimeError("switch did not start listening")


def wait_connections(process, count):
    pending = b""
    connected = 0
    deadline = time.monotonic() + 4
    while connected < count and time.monotonic() < deadline:
        ready, _, _ = select.select([process.stderr], [], [], 0.1)
        if ready:
            pending += os.read(process.stderr.fileno(), 4096)
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                if b"host connected on port" in line:
                    connected += 1
    assert connected == count


def stop(process, clients):
    for client in clients:
        client.close()
    if process.poll() is None:
        process.send_signal(2)
    assert process.wait(timeout=5) == 0
    process.stderr.close()


def expect_no_frame(client):
    client.settimeout(0.25)
    try:
        receive_frame(client)
        raise AssertionError("unexpected frame was delivered")
    except socket.timeout:
        pass


def wait_for_text(process, text):
    pending = b""
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        ready, _, _ = select.select([process.stderr], [], [], 0.1)
        if ready:
            pending += os.read(process.stderr.fileno(), 4096)
            if text in pending:
                return
    raise AssertionError(f"switch did not log {text!r}")


def run_swctl(*arguments):
    result = subprocess.run(["./swctl", *arguments], check=True,
                            capture_output=True, text=True, timeout=3)
    return result.stdout


def test_admin_and_shared_stats():
    port = BASE_PORT + 2
    process = subprocess.Popen([SWITCH_BINARY, str(port), "3"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    clients = []
    try:
        clients.extend(connect_client(port) for _ in range(3))
        wait_connections(process, 3)
        source = bytes.fromhex("020000000081")
        send_frame(clients[0], source, b"\xff" * 6, 1, 1, b"count me")
        receive_frame(clients[1])
        receive_frame(clients[2])

        stats = ""
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            stats = run_swctl("stats")
            if "port 1 in 1" in stats and "port 2 in 0 out 1" in stats:
                break
            time.sleep(0.02)
        ports = run_swctl("show", "ports")
        assert "port 1 up" in ports and "port 2 up" in ports
        assert "in 1" in ports and "out 1" in ports
        assert "port 1 in 1" in stats and "port 2 in 0 out 1" in stats, stats
        macs = run_swctl("show", "mac")
        assert "02:00:00:00:00:81 vlan 1 port 1" in macs
        watch = subprocess.Popen(["timeout", "1.2", "./swctl", "stats", "--watch"],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        watch_output, _ = watch.communicate(timeout=3)
        assert watch.returncode == 124 and "port" in watch_output
        assert run_swctl("shutdown", "2").strip() == "ok"
        clients[1].settimeout(2)
        assert clients[1].recv(1) == b""
        assert run_swctl("up", "2").strip() == "ok"
        replacement = connect_client(port)
        clients.append(replacement)
        wait_for_text(process, b"host connected on port 2")
        assert run_swctl("vlan", "2", "20").strip() == "ok"
        assert "vlan 20" in run_swctl("show", "ports")
        assert run_swctl("mirror", "1", "3").strip() == "ok"
        assert run_swctl("clear", "mac").strip() == "ok"

        process.send_signal(10)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not os.path.exists("mac_table.dump"):
            time.sleep(0.05)
        assert os.path.exists("mac_table.dump")
        os.unlink("mac_table.dump")
        deadline = time.monotonic() + 2
        log = ""
        while time.monotonic() < deadline:
            with open("switch.log", encoding="utf-8") as log_file:
                log = log_file.read()
            if re.search(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d host connected on port 1", log):
                break
            time.sleep(0.02)
        assert "host connected on port 1" in log
    finally:
        stop(process, clients)


def test_vlan_trunk_mirror_and_reload():
    port = BASE_PORT
    config_text = "port 1 vlan 10\nport 2 vlan 20\nport 3 vlan 20\nport 4 trunk\nport 5 vlan 30\nmirror 1 5\n"
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as config:
        config.write(config_text)
        config_path = config.name
    process = subprocess.Popen([SWITCH_BINARY, str(port), "5", config_path],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    clients = []
    try:
        for _ in range(5):
            clients.append(connect_client(port))
        wait_connections(process, 5)
        source = bytes.fromhex("020000000061")
        broadcast = b"\xff" * 6
        send_frame(clients[0], source, broadcast, 10, 1, b"vlan ten")
        expect_no_frame(clients[2])
        assert receive_frame(clients[3])[14:16] == (10).to_bytes(2, "big")
        assert receive_frame(clients[4]).endswith(b"vlan ten")
        expect_no_frame(clients[1])
        expect_no_frame(clients[2])

        with open(config_path, "w", encoding="utf-8") as config:
            config.write("port 1 vlan 10\nport 2 vlan 20\nport 3 vlan 10\nport 4 trunk\nport 5 vlan 30\nmirror 1 5\n")
        process.send_signal(1)
        wait_for_text(process, b"configuration loaded")
        send_frame(clients[0], source, broadcast, 10, 1, b"after reload")
        assert receive_frame(clients[2]).endswith(b"after reload")
    finally:
        stop(process, clients)
        os.unlink(config_path)


def test_rate_limit_uses_refilled_tokens():
    port = BASE_PORT + 1
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as config:
        config.write("port 2 rate 5\n")
        config_path = config.name
    process = subprocess.Popen([SWITCH_BINARY, str(port), "2", config_path],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    clients = []
    try:
        clients.extend((connect_client(port), connect_client(port)))
        wait_connections(process, 2)
        source = bytes.fromhex("020000000071")
        broadcast = b"\xff" * 6
        start = time.monotonic()
        for index in range(6):
            send_frame(clients[0], source, broadcast, 1, 1, f"frame-{index}".encode())
        frames = [receive_frame(clients[1]) for _ in range(6)]
        elapsed = time.monotonic() - start
        assert frames[-1].endswith(b"frame-5")
        assert elapsed >= 0.12, f"rate limit elapsed only {elapsed:.3f}s"
    finally:
        stop(process, clients)
        os.unlink(config_path)


def test_priority_queues_and_aging():
    port = BASE_PORT + 3
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as config:
        config.write("port 2 rate 5\n")
        config_path = config.name
    process = subprocess.Popen([SWITCH_BINARY, str(port), "2", config_path],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    clients = []
    try:
        clients.extend((connect_client(port), connect_client(port)))
        wait_connections(process, 2)
        source = bytes.fromhex("020000000091")
        broadcast = b"\xff" * 6
        send_frame(clients[0], source, broadcast, 1, 0, b"first")
        assert receive_frame(clients[1]).endswith(b"first")
        pending = [(0, f"high-{i}".encode()) for i in range(8)]
        pending.extend((1, f"normal-{i}".encode()) for i in range(3))
        pending.append((2, b"low"))
        for priority, payload in pending:
            send_frame(clients[0], source, broadcast, 1, priority, payload)
        received = [receive_frame(clients[1]) for _ in pending]
        payloads = [frame[17:] for frame in received]
        assert b"low" in payloads[:9], f"low-priority frame starved: {payloads}"
        assert sum(payload.startswith(b"high-") for payload in payloads) >= 5
        assert sum(payload.startswith(b"normal-") for payload in payloads) >= 2
    finally:
        stop(process, clients)
        os.unlink(config_path)


def main():
    test_vlan_trunk_mirror_and_reload()
    test_rate_limit_uses_refilled_tokens()
    test_priority_queues_and_aging()
    test_admin_and_shared_stats()


if __name__ == "__main__":
    main()
