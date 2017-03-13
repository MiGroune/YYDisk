# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from   ctypes import *
import numpy as np
import struct
import binascii
import string
from pyspark import SparkContext


class x3c_timeval(Structure):
    _fields_ = [
        ("tv_sec",  c_int32),
        ("tv_nsec", c_int32)
    ]


class packet_hdr_t(Structure):
    _fields_ = [
        ("ts",     x3c_timeval),
        ("length", c_uint32)
    ]


class log_glbhdr_t(Structure):
    _fields_ = [
        ("magic_number",  c_uint32),
        ("version_major", c_uint16),
        ("version_minor", c_uint16),
        ("if_type",       c_uint16),
        ("if_enum",       c_uint16),
        ("ts",            x3c_timeval),
        ("thiszone",      c_int32),
        ("opt",           c_uint32 * 2),
    ]


class T51DataHandle(object):
    def __init__(self, filename):
        self.m_globalHeader = log_glbhdr_t()
        self.m_file = open(filename, 'rb')
        self.getFileLength()


    def getFileLength(self):
        self.m_file.seek(0,2);
        self.m_fileLength = self.m_file.tell()
        self.m_file.seek(0,0)

    def readGlobalHeader(self):
        #read magic number
        self.magic_number  = self.readDataBy(4)

        #read major version number        
        self.version_major = self.readDataBy(2)

        #read minor version number
        self.version_minor = self.readDataBy(2)

        #read interface Type
        self.if_type = self.readDataBy(2)

        #read interface enumerator
        self.if_enum = self.readDataBy(2)

        #read time struct
        self.ts = x3c_timeval()
        self.ts.tv_sec = self.readDataBy(4, '-')
        self.ts.tv_nsec = self.readDataBy(4, '-')

        #read GMT to local correction
        self.thiszone = self.readDataBy(4, '-')

        #read optional data field
        self.opt = []
        opt0 = self.readDataBy(4, '-')
        opt1 = self.readDataBy(4, '-')
        self.opt.append(opt0)
        self.opt.append(opt1)

    def readPackHeader(self):
        tv_sec  = self.readDataBy(4, '-')
        tv_nsec = self.readDataBy(4, '-')
        length  = self.readDataBy(4)
        return tv_sec, tv_nsec, length

    def collectPackedData(self):
        allDataList = []
        count = 0
        currentPos = self.m_file.tell()

        while currentPos != self.m_fileLength:
            try:
                allDataList.append(self.readPerPackedData())
                count += 1
                currentPos = self.m_file.tell()
                print "PackedData Num: " + str(count) + ", Current Position: " + str(currentPos) + ", Remain: " + str(
                    self.m_fileLength-currentPos)
            except Exception, e:
                print e
                break

        pass


    def readPerPackedData(self):
        packHeader = self.readPackHeader()
        dataList = []
        for indix in range(1, packHeader[2],2):
            dataList.append(self.readDataBy(2))
        return packHeader, dataList

    def parseData(self):
        self.readGlobalHeader()
        self.collectPackedData()

    def readDataBy(self, length, sign = '+', endian = False):
        dtype = ''

        if endian == False:
            dtype = '<'
        else:
            dtype = '>'

        if sign == '-':
            dtype += 'i'
        else:
            dtype += 'u'

        dtype += str(length)
        data = np.ndarray(shape=(1,), dtype=dtype, buffer=self.m_file.read(length))
        return data[0]



if __name__ == '__main__':
"""
    logFile = 'C:\HANDY_v2p3\README.md'
    sc = SparkContext("local","Simple App")
    #ldata = sc.binaryRecords(logFile)
    logData = sc.textFile(logFile).cache()


    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print("Lines with a: %i, lines with b: %i"%(numAs, numBs))
"""
    T51data = T51DataHandle("T51_E0_R0_2017_01_19_004250.lf1")
    T51data.parseData()


    currentByte = f.read(4)
    hexValue = binascii.hexlify(currentByte)
    data = string.atoi(hexValue)

"""
    hexs = str(hexValue)
    pack_data = struct.pack('<L', hexValue)
    if len(currentByte) == 0:
        break;
    else:
        print "%2s" % currentByte.encode('hex'),
        index = index + 1
        if index == 16:
            index = 0
            print

f.close()
"""