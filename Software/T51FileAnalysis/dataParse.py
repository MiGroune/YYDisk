# -*- coding: utf-8 -*-
"""
This file is used for data analyses
Data Format Support:
1. LF1
2.
3.

"""
from LF1Structure import *
from ctypes import *
from pyspark import SparkContext
import numpy as np
import struct
import binascii
import string


class T51DataHandle(object):
    def __init__(self, filename):
        self.m_globalHeader = log_glbhdr_t()
        self.m_file = open(filename, 'rb')
        self.getFileLength()

        self.debug = True


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
                if self.debug == True:
                    print "PackedData Num: " + str(count) + ", Current Position: " + str(currentPos) + ", Remain: " + str(
                        self.m_fileLength-currentPos)
            except Exception, e:
                print e
                break

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
    T51data = T51DataHandle("T51_E0_R0_2017_01_19_004250.lf1")
    T51data.parseData()