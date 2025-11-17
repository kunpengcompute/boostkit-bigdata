#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
KMC类
"""

import ctypes
import common.kmc_toolkit.constants as CONSTANTS
from common.kmc_toolkit.KmcWrapperAPI import KmcWrapper
from common.kmc_toolkit.msg_type import KmcConfig
from common.kmc_toolkit.log import kmc_logger
from common.kmc_toolkit.log import logger
from common import kmc_settings


@ctypes.CFUNCTYPE(None, ctypes.c_int, ctypes.c_char_p)
def kmc_logger_callback(level, msg):
    """
    设置回调日志函数
    :param level: 日志级别
    :param msg: 日志内容
    :return:
    """
    if level < CONSTANTS.LOG_LEVEL:
        kmc_logger.info(msg.decode("utf-8"), extra={"skip": True})


class Kmc:
    """
    KMC类，对外提供的类
    """

    def __new__(cls):
        """
        单例模式，新建类
        """
        if not hasattr(Kmc, "_instance"):
            Kmc._instance = object.__new__(cls)
        return Kmc._instance

    def __init__(self):
        """
        构造函数，单例模式
        """
        if not hasattr(Kmc, "_first_init"):
            Kmc._first_init = True
            self.kmc_wrapper = KmcWrapper()
        self.domain_id = CONSTANTS.DOMAIN_ID

    def initialize(self):
        """
        初始化配置
        :return:
        """
        # set logger function callback
        self.kmc_wrapper.set_logger(kmc_logger_callback, CONSTANTS.LOG_LEVEL)
        logger.info("Kmc set log success.")

        # set ksf file path
        primary_ksf = CONSTANTS.KMC_KSF_PRIMARY
        standby_ksf = CONSTANTS.KMC_KSF_STANDBY

        # initial sem key
        sem_key = CONSTANTS.DEFAULT_SEM_KEY + kmc_settings.KMC_SEM_KEY

        kmc_config = KmcConfig(
            primary_ksf.encode("utf-8"),
            standby_ksf.encode("utf-8"),
            CONSTANTS.DOMAIN_COUNT,
            CONSTANTS.KMC_ROLE_MASTER,
            CONSTANTS.PROC_LOCK_PERM,
            CONSTANTS.SDP_ALG_ID,
            CONSTANTS.HMAC_AIG_ID,
            sem_key,
            CONSTANTS.INNER_SYMM_AI_GID,
            CONSTANTS.INNER_HASH_AI_GID,
            CONSTANTS.INNER_HMAC_AIG_ID,
            CONSTANTS.INNER_KDF_AI_GID,
            CONSTANTS.WORK_KEY_ITER,
            CONSTANTS.ROOT_KEY_ITER,
            CONSTANTS.VERSION,
        )
        ret = self.kmc_wrapper.set_config(kmc_config)
        if ret != CONSTANTS.RET_SUCCESS:
            logger.error("Kmc set config failed, ret: %s", ret)
            return CONSTANTS.INITIALIZE_FAILED

        # active new master key
        ret = self.active_new_mk()
        if ret != CONSTANTS.RET_SUCCESS:
            logger.error("Kmc active master key failed, ret: %s", ret)
            return CONSTANTS.ACTIVE_NEW_MK_FAILED

        # check and update master key
        try:
            self.kmc_wrapper.check_and_update_task(
                self.domain_id, CONSTANTS.DEFAULT_MAX_KEYLIFE_DAYS - CONSTANTS.LIFETIME_DAYS
            )
        except Exception as err:
            logger.error("encryption module check and update MK task failed, catch err: %s", err)

        return CONSTANTS.RET_SUCCESS

    def finalize(self):
        """
        析构函数，释放资源
        :return:
        """
        try:
            self.kmc_wrapper.finalize()
        except Exception as err:
            logger.error("destruct encryption module failed, catch err: %s", err)

    def active_new_mk(self):
        """
        创建新的主密钥key
        :return:
        """
        ret, max_key_id = self.kmc_wrapper.get_max_mk_id(self.domain_id)
        if ret != CONSTANTS.RET_SUCCESS:
            logger.info("Kmc get max MK ID failed")
            return CONSTANTS.GET_MAX_MK_ID_FAILED
        if int(max_key_id.value) <= 0:
            if self.kmc_wrapper.active_new_mk(self.domain_id) != CONSTANTS.RET_SUCCESS:
                logger.error("Kmc active MK failed")
                return CONSTANTS.ACTIVE_NEW_MK_FAILED

        return CONSTANTS.RET_SUCCESS

    def encrypt(self, plain_text):
        """
        加密
        :param plain_text: 待加密字符
        :return:
        """
        if plain_text == "":
            logger.warning("cipher is null.")
            return CONSTANTS.ENCRPT_FAILED, None

        ret = CONSTANTS.ENCRPT_FAILED
        cipher_text = ""

        if self.initialize() != CONSTANTS.RET_SUCCESS:
            logger.error("encryption module initialization fail.")
            return ret, cipher_text

        try:
            ret, cipher_text = self.kmc_wrapper.encrypt(self.domain_id, plain_text)
        except Exception as err:
            logger.error("Encrypt failed, catch err: %s", err)
        finally:
            self.finalize()

        if ret == CONSTANTS.RET_SUCCESS:
            logger.info("encrypt success.")
        else:
            logger.error("encrypt failed.")
        del plain_text
        return ret, cipher_text

    def decrypt(self, cipher_text):
        """
        解密
        :param cipher_text: 密文
        :return:
        """
        if cipher_text == "":
            logger.warning("cipher is null.")
            return CONSTANTS.DECRPT_FAILED, None

        ret = CONSTANTS.DECRPT_FAILED
        plain_text = ""

        try:
            self.initialize()
            ret, plain_text = self.kmc_wrapper.decrypt(self.domain_id, cipher_text)
        except Exception as err:
            logger.error("Encrypt failed, catch err: %s", err)
        finally:
            self.finalize()

        if ret == CONSTANTS.RET_SUCCESS:
            logger.info("decrypt success.")
        else:
            logger.error("decrypt failed.")
        del cipher_text
        return ret, plain_text


kmc = Kmc()
