from socket import *
import socket
import threading
import logging
import time
import sys
import multiprocessing

from concurrent.futures import ProcessPoolExecutor

from file_protocol import FileProtocol
fp = FileProtocol()

class ProcessTheClient(multiprocessing.Process):
    def __init__(self, connfd, address):
        super().__init__()
        self.connfd = connfd
        self.address = address

    def run(self):
        conn = socket.fromfd(self.connfd, socket.AF_INET, socket.SOCK_STREAM)
        buffer = b""
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while b"\r\n\r\n" in buffer or b"\n\n" in buffer:
                if b"\r\n\r\n" in buffer:
                    delimiter = b"\r\n\r\n"
                else:
                    delimiter = b"\n\n"
                parts = buffer.split(delimiter, 1)
                d = parts[0].decode()
                # Submit to the process pool
                hasil = fp.proses_string(d)
                hasil = hasil + "\r\n\r\n"
                conn.sendall(hasil.encode())
                buffer = parts[1]
        conn.close()

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=6666):
        self.ipinfo = (ipaddress, port)
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        while True:
            conn, client_address = self.my_socket.accept()
            logging.warning(f"connection from {client_address}")
            # Pass file descriptor to child process
            p = ProcessTheClient(conn.fileno(), client_address)
            p.start()
            conn.close() 

def main():
    svr = Server(ipaddress='0.0.0.0', port=6666)
    svr.run()
    logging.warning("Server telah selesai menjalankan semua tugas.")

if __name__ == "__main__":
    main()

