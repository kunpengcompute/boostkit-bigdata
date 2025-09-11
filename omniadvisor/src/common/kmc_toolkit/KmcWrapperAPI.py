#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
KMC包装类，调用KMC C库实现类
"""

import ctypes
import traceback
import platform
import common.kmc_toolkit.constants as CONSTANTS
from common.kmc_toolkit.msg_type import KmcConfig
from common.kmc_toolkit.log import logger


class KmcWrapper:
    """
    KMC包装类，调用KMC C库实现类
    """

    def __new__(cls):
        """
        单例模式，新建类
        """
        if not hasattr(KmcWrapper, "_instance"):
            KmcWrapper._instance = object.__new__(cls)
        return KmcWrapper._instance

    def __init__(self):
        """
        构造函数
        """
        if not hasattr(KmcWrapper, "_first_init"):
            KmcWrapper._first_init = True
        try:
            if platform.system().lower() != "windows":
                ctypes.cdll.LoadLibrary(CONSTANTS.LINUX_KMC_CRYPTO_SO_PATH)
                ctypes.cdll.LoadLibrary(CONSTANTS.LINUX_KMC_SECUREC_SO_PATH)
                ctypes.cdll.LoadLibrary(CONSTANTS.LINUX_KMC_SO_PATH)
                ctypes.cdll.LoadLibrary(CONSTANTS.LINUX_KMC_SDP_SO_PATH)
                self.kmc_lib = ctypes.cdll.LoadLibrary(CONSTANTS.LINUX_KMC_EXT_SO_PATH)
            else:
                self.kmc_lib = ctypes.cdll.LoadLibrary(CONSTANTS.WINDOWS_KMC_EXT_SO_PATH)
        except Exception as err:
            logger.error("load so failed, catch err: %s", err)

    def finalize(self):
        """
        析构函数
        :return:
        """
        ret = CONSTANTS.FINALIZE_FAILED
        try:
            ret = self.kmc_lib.KeFinalize()
        except Exception as err:
            logger.error("KeFinalize failed, catch err: %s", err)

        return ret

    def set_logger(self, kmc_logger_cb, logger_level):
        """
        设置日志函数
        :param kmc_logger_cb: 日志回调函数
        :param logger_level: 日志级别
        :return:
        """
        try:
            self.kmc_lib.KeSetLoggerCallback(kmc_logger_cb)
            self.kmc_lib.KeSetLoggerLevel(logger_level)
        except Exception as err:
            logger.error("set_logger failed, catch err: %s", err)

    def set_config(self, config):
        """
        初始化配置
        :param config: 配置
        :return:
        """
        ret = CONSTANTS.INITIALIZE_FAILED
        try:
            ret = self.kmc_lib.KeInitialize(ctypes.POINTER(KmcConfig)(config))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error("KeGetMaxMkID failed, catch err: %s", err)

        return ret

    def get_max_mk_id(self, domain_id):
        """
        获取最大的key id
        :param domain_id: 域值（每个域有多个密钥）
        :return:
        """
        max_key_id = ctypes.c_uint()
        ret = CONSTANTS.GET_MAX_MK_ID_FAILED

        try:
            ret = self.kmc_lib.KeGetMaxMkID(ctypes.c_uint(domain_id), ctypes.pointer(max_key_id))
        except Exception as err:
            logger.error("KeGetMaxMkID failed, catch err: %s", err)

        return ret, max_key_id

    def active_new_mk(self, domain_id):
        """
        创建一个新的密钥
        :param domain_id: 域值（每个域有多个密钥）
        :return:
        """
        ret = CONSTANTS.ACTIVE_NEW_MK_FAILED
        try:
            ret = self.kmc_lib.KeActiveNewKey(ctypes.c_uint(domain_id))
        except Exception as err:
            logger.error("KeActiveNewKey failed, catch err: %s", err)

        return ret

    def check_and_update_task(self, domain_id, advance_days):
        """
        定期检查和更新密钥
        :param domain_id: 域值（每个域有多个密钥）
        :param advance_days: 更新周期
        :return:
        """
        refresh_mask_ret = CONSTANTS.REFRESH_MASK_FAILED
        update_mk_ret = CONSTANTS.UPDATE_MK_FAILED
        try:
            refresh_mask_ret = self.kmc_lib.KeRefreshMkMask()
        except Exception as err:
            logger.error("refresh MK mask failed, catch error: %s", err)

        if refresh_mask_ret:
            logger.error("refresh MK mask failed, ret: %d", refresh_mask_ret)
            return

        try:
            update_mk_ret = self.kmc_lib.KeCheckAndUpdateMk(ctypes.c_uint(domain_id), ctypes.c_int(advance_days))
        except Exception as err:
            logger.error("check_and_updat MK failed, catch err: %s", err)

        if update_mk_ret:
            logger.error("check_and_updat MK failed, ret:%d", update_mk_ret)

    def encrypt(self, domain_id, content):
        """
        加密
        :param domain_id: 域值（每个域有多个密钥）
        :param content: 待加密内容
        :return:
        """
        plain_text_len = len(content)
        plain_text = content.encode("utf-8")
        plcipher_text_len = ctypes.c_int()
        cipher_text = ctypes.c_char_p()
        ret = CONSTANTS.ENCRPT_FAILED

        try:
            ret = self.kmc_lib.KeEncryptByDomain(
                ctypes.c_uint(domain_id),
                ctypes.c_char_p(plain_text),
                ctypes.c_int(plain_text_len),
                ctypes.pointer(cipher_text),
                ctypes.pointer(plcipher_text_len),
            )

        except Exception as err:
            logger.error("encrypt failed, catch err: %s", err)

        cipher_value = cipher_text.value.decode("utf-8", "ignore")
        del content
        del plain_text
        del cipher_text
        return ret, cipher_value

    def decrypt(self, domain_id, content):
        """
        解密
        :param domain_id: 域值（每个域有多个密钥）
        :param content: 待解密内容
        :return:
        """
        plcipher_text_len = len(content)
        cipher_text = content.encode("utf-8")
        plain_text_len = ctypes.c_int()
        plain_text = ctypes.c_char_p()
        ret = CONSTANTS.DECRPT_FAILED

        try:
            ret = self.kmc_lib.KeDecryptByDomain(
                ctypes.c_uint(domain_id),
                ctypes.c_char_p(cipher_text),
                ctypes.c_int(plcipher_text_len),
                ctypes.pointer(plain_text),
                ctypes.pointer(plain_text_len),
            )
        except Exception as err:
            logger.error("Decrypt failed, catch err: %s", err)

        plain_value = plain_text.value.decode("utf-8", "ignore")
        del content
        del cipher_text
        del plain_text
        return ret, plain_value