import socket
import json
import base64
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# logging
import csv
import os 
import time
import threading

server_address=('0.0.0.0',7777)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    logging.warning(f"connecting to {server_address}")

    # Command delimiter
    if not command_str.endswith("\r\n\r\n"):
        command_str += "\r\n\r\n"
    try:
        logging.warning(f"sending message ")
        sock.sendall(command_str.encode())
        # Look for the response, waiting until socket is done (no more data)
        data_received="" #empty string
        while True:
            #socket does not receive all data at once, data comes in part, need to be concatenated at the end of process
            data = sock.recv(4096)
            if data:
                #data is not empty, concat with previous content
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                # no more data, stop the process by break
                break
        # at this point, data_received (string) will contain all data coming from the socket
        # to be able to use the data_received as a dict, need to load it using json.loads()
        hasil = json.loads(data_received)
        logging.warning("data received from server:")
        return hasil
    except:
        logging.warning("error during data receiving")
        return False


def remote_list():
    command_str=f"LIST"
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        print("daftar file : ")
        for nmfile in hasil['data']:
            print(f"- {nmfile}")
        return True
    else:
        print("Gagal")
        return False

def remote_get(filename=""):
    command_str=f"GET {filename}"
    hasil = send_command(command_str)
    if (hasil['status']=='OK'):
        #proses file dalam bentuk base64 ke bentuk bytes
        namafile= hasil['data_namafile']
        isifile = base64.b64decode(hasil['data_file'])
        fp = open(namafile,'wb+')
        fp.write(isifile)
        fp.close()
        print(f"Berhasil Mendapatkan {namafile}")
        return True
    else:
        print("Gagal")
        return False

def remote_upload(filepath=""):
    filecontent = f"{convert_file(filepath)}"
    command_str=f"UPLOAD {filepath} {filecontent}"
    hasil = send_command(command_str)
    if(hasil['status']=='OK'):
        print(f"Berhasil Mengupload {filepath}")
        return True
    else:
        print(f"Gagal Mengupload {filepath}")
        return False

def convert_file(filepath):
    try:
        with open(filepath, 'rb') as f:
            encoded = base64.b64encode(f.read())
            return encoded.decode("utf-8")
    except Exception as e:
        print(f"Gagal mengonversi file: {e}")
        return False


if __name__=='__main__':
    server_address=('0.0.0.0',6666)
    client_workers = 1
    tasks = [
        ('list', None),
        ('get', 'file_10mb-b.bin'),
        ('upload', 'file_10mb-a.bin'),
    ]
    sukses_get = 0
    gagal_get = 0
    sukses_upload = 0
    gagal_upload = 0
    bytes_get = 0
    bytes_upload = 0
    waktu_get = 0
    waktu_upload = 0

    start_time = time.time()
    with ProcessPoolExecutor(max_workers=client_workers) as executor:
        futures = []
        waktu_mulai_get = waktu_mulai_upload = None
        waktu_selesai_get = waktu_selesai_upload = None
        for operasi, arg in tasks:
            if operasi == 'list':
                futures.append((operasi, arg, executor.submit(remote_list)))
            elif operasi == 'get':
                futures.append((operasi, arg, executor.submit(remote_get, arg)))
            elif operasi == 'upload':
                futures.append((operasi, arg, executor.submit(remote_upload, arg)))
        
        for operasi, arg, f in futures:
            t0 = time.time()
            result = f.result()
            t1 = time.time()
            if operasi == 'get':
                if waktu_mulai_get is None:
                    waktu_mulai_get = t0
                waktu_selesai_get = t1
                if result:
                    sukses_get += 1
                    if arg and os.path.exists(arg):
                        bytes_get += os.path.getsize(arg)
                else:
                    gagal_get += 1
            elif operasi == 'upload':
                if waktu_mulai_upload is None:
                    waktu_mulai_upload = t0
                waktu_selesai_upload = t1
                if result:
                    sukses_upload += 1
                    if arg and os.path.exists(arg):
                        bytes_upload += os.path.getsize(arg)
                else:
                    gagal_upload += 1

    waktu_get = (waktu_selesai_get - waktu_mulai_get) if waktu_mulai_get and waktu_selesai_get else 0
    waktu_upload = (waktu_selesai_upload - waktu_mulai_upload) if waktu_mulai_upload and waktu_selesai_upload else 0
    throughput_get = bytes_get / waktu_get if waktu_get > 0 else 0
    throughput_upload = bytes_upload / waktu_upload if waktu_upload > 0 else 0

    # Logging ke CSV
    with open('client_log.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['operasi', 'waktu_total', 'throughput', 'sukses', 'gagal'])
        writer.writerow(['get', waktu_get, throughput_get, sukses_get, gagal_get])
        writer.writerow(['upload', waktu_upload, throughput_upload, sukses_upload, gagal_upload])
    print(f"Log sesi client ditulis ke client_log.csv")