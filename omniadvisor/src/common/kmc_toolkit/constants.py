#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
常量类
"""
import os
import platform

# kmc comfig param item
ROOT_DIR_PATH = os.path.realpath(os.path.join(os.path.split(os.path.realpath(__file__))[0], ".."))
LINUX_KMC_PATH = os.path.realpath(os.path.sep.join([ROOT_DIR_PATH, "3rd", "release", "lib", "linux"]))
LINUX_KMC_EXT_SO = "libkmcext.so.23.0.0"
LINUX_KMC_SO = "libkmc.so.23.0.0"
LINUX_KMC_SDP_SO = "libsdp.so.23.0.0"
LINUX_KMC_CRYPTO_SO = "libcrypto.so.1.1"
LINUX_KMC_SECUREC_SO = "libsecurec.so"
LINUX_KMC_CRYPTO_SO_PATH = os.path.realpath(os.path.sep.join([LINUX_KMC_PATH, LINUX_KMC_CRYPTO_SO]))
LINUX_KMC_SECUREC_SO_PATH = os.path.realpath(os.path.sep.join([LINUX_KMC_PATH, LINUX_KMC_SECUREC_SO]))
LINUX_KMC_SDP_SO_PATH = os.path.realpath(os.path.sep.join([LINUX_KMC_PATH, LINUX_KMC_SDP_SO]))
LINUX_KMC_SO_PATH = os.path.realpath(os.path.sep.join([LINUX_KMC_PATH, LINUX_KMC_SO]))
LINUX_KMC_EXT_SO_PATH = os.path.realpath(os.path.sep.join([LINUX_KMC_PATH, LINUX_KMC_EXT_SO]))

WINDOWS_KMC_SO_PATH = os.path.realpath(os.path.sep.join([ROOT_DIR_PATH, "3rd", "release", "lib", "windows"]))
WINDOWS_KMC_EXT_SO = "kmcext.dll"
WINDOWS_KMC_EXT_SO_PATH = os.path.realpath(os.path.sep.join([WINDOWS_KMC_SO_PATH, WINDOWS_KMC_EXT_SO]))

KMC_KSF_PATH = "../"
KMC_KSF_PRIMARY = os.path.realpath(os.path.sep.join([ROOT_DIR_PATH, "conf", "kmc", "primary", "primaryKeyStoreFile"]))
KMC_KSF_STANDBY = os.path.realpath(os.path.sep.join([ROOT_DIR_PATH, "conf", "kmc", "standby", "standbyKeyStoreFile"]))
DOMAIN_ID = 0
DOMAIN_COUNT = 8
KMC_ROLE_MASTER = 1
KMC_ROLE_AGENT = 0
PROC_LOCK_PERM = 0o660
SDP_ALG_ID = 8
HMAC_AIG_ID = 2052
DEFAULT_SEM_KEY = 0x20160000
MIN_HEX_SEM_KEY = 0x1111
MAX_HEX_SEM_KEY = 0x9999
INNER_SYMM_AI_GID = 0
INNER_HASH_AI_GID = 0
INNER_HMAC_AIG_ID = 0
INNER_KDF_AI_GID = 0
WORK_KEY_ITER = 10000
ROOT_KEY_ITER = 10000

SEC_PATH_WINDOWS_MAX = 248
SEC_PATH_LINUX_MAX = 4096
SEC_PATH_MAX = SEC_PATH_WINDOWS_MAX if platform.system().lower() == "windows" else SEC_PATH_LINUX_MAX
PATH_SEPERATOR = "/"

# ret enum Item
RET_SUCCESS = 0
RET_FAILED = 1
INITIALIZE_FAILED = 2
GET_MAX_MK_ID_FAILED = 3
ACTIVE_NEW_MK_FAILED = 4
ENCRPT_FAILED = 5
DECRPT_FAILED = 6
FINALIZE_FAILED = 7

# check master key  param item
INTERVAL_TIMER = 3600
LIFETIME_DAYS = 10
DEFAULT_MAX_KEYLIFE_DAYS = 180

# log param item
LOG_LEVEL = 3

# plain text length limit
TEXT_LENGTH = 512

REFRESH_MASK_FAILED = 8
UPDATE_MK_FAILED = 9
FREE_MEMORY_FAILED = 10
VERSION = 0