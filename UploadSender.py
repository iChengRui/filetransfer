#!/usr/bin/env python3
# -*- coding: utf8 -*-


"""
use multi-thread  to transfer file
this is a sender
under Python3.6, compatibility isn't considered!
"""

import concurrent.futures as furures
import math
import mmap
import os
import pathlib
import socket
import argparse
import sys


def sender(ip: str, port: int, source: object = str,
           max_thread: object = int,
           max_size_slice: object = int) -> None:
    """
    send file by divide it with multi-thread
    
    1M=1048576bytes
        
    :type source: object
    :param ip: peer IP
    :param port: peer port
    :param source: file need to be sent
    :param max_thread: max number of thread
    :param max_size_slice: per file size
    :return: None 
    """
    max_size_slice *= 1048576
    file_name = pathlib.PurePath(source).name
    file_size = os.path.getsize(source)
    num_slice = math.ceil(file_size / max_size_slice)
    buf = [file_name, str(num_slice), '\n']
    buf = ",".join(buf).encode()

    slc = list(range(0, file_size, max_size_slice))
    slc.append(file_size)
    remained = slc = list(enumerate(zip(slc[:-1], slc[1:])))
    sct = socket.socket()
    sct.connect((ip, port))
    while True:
        sct.send(buf)
        if sct.recv(4096) == b"\n":
            break
    file_num = os.open(source, os.O_RDONLY)
    buf = mmap.mmap(file_num, file_size, mmap.MAP_SHARED,
                    mmap.PROT_READ)
    with furures.ThreadPoolExecutor(max_workers=max_thread) as thdpl:
        while True:
            for order, (start, end) in remained:
                thdpl.submit(send, ip, port, buf[start:end], order)
            buf = sct.recv(4096)
            if buf != b"\n":
                remained = []
                for i in buf.decode().split(","):
                    remained.append(slc[int(i)])
            else:
                break
        thdpl.shutdown(wait=True)
    os.close(file_num)


def send(ip, port, buf, order):
    """
    send part of file  with a thread
    :param ip: peer IP
    :param port: peer port 
    :param buf: part of file ,as a mmap
    :param num: the place it in the file
    :return: None
    """
    sct = socket.socket()
    sct.connect((ip, port))
    buffer = str(order) + "," + str(len(buf)) + "\n"
    while True:
        sct.send(buffer.encode())
        if sct.recv(4096) == b"\n":
            break
    send_count = 0
    file_size = len(buf)
    while True:
        i = sct.send(buf[send_count:])
        send_count += i
        if send_count == file_size:
            break
    sct.close()


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
    option.add_argument('-s', '--SLICESIZE', help="文件分片后每片大小",
                        action='store', required=True)
    option.add_argument('-t', '--THREAD', help="同时开启的线程数量",
                        action='store', required=True)

    parsedoption = option.parse_args(parmlist)

    return parsedoption


if __name__ == "__main__":

    if len(sys.argv) != 6:
        print("Enter some information,which is split by space")
        param = input("IP PORT FILEPATH SLICESIZE THREAD:")
        ip, port, filepath, slicesize, threadnum = param.split()
    else:
        param = get_option(sys.argv[1:])
        ip = param.IP
        port = param.PORT
        filepath = param.FILEPATH
        slicesize=param.SLICESIZE
        threadnum=param.THREAD

    sender(ip, int(port), filepath, int(slicesize), int(threadnum))
