#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""
use multi-thread  to transfer file
including both sender and receiver
under Python3.6, compatibility isn't considered!
"""

import argparse
import os
import pathlib
import socket
import sys
import tempfile
import threading
import hashlib
import  time
import shutil
lck = threading.Lock()
evnt = threading.Event()
file_received = set()
file_receiving = set()


def receiver(ip, port, target_loc=None):
    """
    receive a file with multi-thread
    :param ip: the listening IP
    :param port: the listening PORT
    :param target_loc: location of file
    :return: None
    """
    sct = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sct.bind((ip, port))
    sct.listen(5)
    initialed = False
    thd_list = []
    total = None
    while True:
        conn, addr = sct.accept()
        if not initialed:
            ctrl_conn = conn
            buf = []
            end = -1
            while end == -1:
                bys = ctrl_conn.recv(4096)
                end = bys.find(b'\n')
                buf.append(bys)
            file_name, total, _ = b"".join(buf).decode("utf8").split(sep=',')
            total_thd = total = int(total)
            temp_dir = pathlib.Path(tempfile.mkdtemp(), file_name)
            os.mkdir(str(temp_dir))
            file_need = set(str(x) for x in range(0, total))
            initialed = True
            ctrl_conn.send(b"\n")
            # print(file_need,total_thd)
            # sys.exit(0)
        else:
            thd = threading.Thread(group=None, target=receive,
                                   args=(conn, temp_dir))
            thd.start()
            thd_list.append(thd)

            if len(thd_list) >= total_thd:
                while True:
                    if len(file_received) == total:
                        break
                    evnt.wait()
                    file_remain = file_need - file_received - file_receiving
                    # print(total_thd, ":", file_received, ",", file_receiving,
                    #      ",", file_remain)
                    # time.sleep(180)
                if file_remain:
                    total_thd = len(file_remain)
                    file_remain = ",".join(file_remain)
                    file_remain += "\n"
                    ctrl_conn.send(file_remain.encode())
                    break
                else:
                    for i in thd_list:
                        i.join()
                        thd_list.remove(i)
            if len(file_received) == total:
                break

    ctrl_conn.send(b"\n")
    ctrl_conn.close()
    sct.close()
    with open(pathlib.Path(target_loc, file_name), 'wb') as f:
        for i in range(0, total):
            with open(pathlib.Path(temp_dir, str(i)), 'rb') as j:
                f.write(j.read())
        shutil.rmtree(str(temp_dir))


def receive(conn, tmp_dir=None):
    """
    receive part of file as a thread

    :param conn: peer connected socket
    :param tmp_dir: where the part locate
    :return: None
    """
    order = []
    global file_receiving, file_received
    evnt.clear()
    while True:
        bys = conn.recv(4096)
        order.append(bys)
        if bys.find(b"\n") != -1:
            break
    order, file_size,digest = b"".join(order).decode("utf8").strip().split(",")
    file_size = int(file_size)
    file_path = pathlib.PurePath(tmp_dir, order)
    md5 = hashlib.md5()
    with open(file_path, 'wb') as f:
        with lck:
            file_receiving.add(order)
            evnt.clear()
        conn.send(b"\n")
        while True:
            bys = conn.recv(4096)
            if not bys:
                break
            f.write(bys)
            md5.update(bys)


    if os.path.getsize(file_path) != file_size or md5.hexdigest()!= digest:
        print("origin:" ,len(digest),"get:",len(md5.hexdigest()))
        print("origin:",file_size,"get:",os.path.getsize(file_path))
        print("origin:",digest,"get:",md5.hexdigest())
        os.remove(str(file_path))
        with lck:
            file_receiving.remove(order)
            evnt.set()
    else:
        with lck:
            file_receiving.remove(order)
            file_received.add(order)
            evnt.set()
    conn.close()


def get_option(parmlist):
    """
    get the parameter to start with.
    :param parmlist: the options you choose, alias argv[1:].
     """
    option = argparse.ArgumentParser(description="file receiver")

    option.add_argument('-i', '--IP', help="IP地址", action='store',
                        required=True)
    option.add_argument('-p', '--PORT', help="端口", action='store',
                        required=True)
    option.add_argument('-f', '--FILEPATH', help="文件路径（发送端需含文件名）",
                        action='store', required=True)

    parsedoption = option.parse_args(parmlist)

    return parsedoption


if __name__ == "__main__":

    if len(sys.argv) != 7:
        print("Enter some information,which is split by space")
        param = input("IP PORT FILEPATH:")
        ip, port, filepath = param.split()
    else:
        param = get_option(sys.argv[1:])
        ip = param.IP
        port = param.PORT
        filepath = param.FILEPATH

    receiver(ip, int(port), filepath)
