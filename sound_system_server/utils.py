import socket

def get_ip_address():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(('10.255.255.255', 1234))
        return sock.getsockname()[0]
    except OSError:
        pass
