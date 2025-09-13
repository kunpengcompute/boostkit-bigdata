from common.kmc_toolkit.KmcUtil import Kmc
from common.kmc_toolkit.log import logger


class TestConstant:
    def test_kmc_util(self):
        # TODO 这个测试用例待完善
        kmc = Kmc()
        logger.info(kmc.decrypt(kmc.encrypt("huawei12313")[1]))
        logger.info(kmc.encrypt("huawei12313"))
        logger.info(kmc.encrypt("12312312312xxx"))
        logger.info(kmc.encrypt("bbbbbb"))