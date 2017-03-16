# -*- coding: utf-8 -*-
"""
This file describes LF1 data Format

"""
from ctypes import *

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