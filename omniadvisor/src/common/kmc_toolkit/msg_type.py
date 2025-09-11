#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
KMC配置类结构体
"""

import ctypes
import common.kmc_toolkit.constants as CONSTANTS


class KmcConfig(ctypes.Structure):
    """
    KMC配置类结构体
    """
    _pack_ = 4
    _fields_ = [
        ("primaryKeyStoreFile", ctypes.c_char * CONSTANTS.SEC_PATH_MAX),
        ("standbyKeyStoreFile", ctypes.c_char * CONSTANTS.SEC_PATH_MAX),
        ("domainCount", ctypes.c_int),
        ("role", ctypes.c_int),
        ("procLockPerm", ctypes.c_int),
        ("sdpAlgId", ctypes.c_int),
        ("hmacAlgId", ctypes.c_int),
        ("semKey", ctypes.c_int),
        ("innerSymmAlgId", ctypes.c_int),
        ("innerHashAlgId", ctypes.c_int),
        ("innerHmacAlgId", ctypes.c_int),
        ("innerKdfAlgId", ctypes.c_int),
        ("workKeyIter", ctypes.c_int),
        ("rootKeyIter", ctypes.c_int),
        ("version", ctypes.c_ushort),
    ]