from types import SimpleNamespace
from unittest.mock import patch

import pytest

from common.constant import check_oa_conf

OA_CONF_PATH = 'common.constant.OA_CONF'


class TestConstant:

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_normal(self):
        # 不应抛异常
        check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        # invalid
        tuning_retest_times=0,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_invalid_tuning_retest_times(self):
        with pytest.raises(ValueError, match='tuning retest times'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        # invalid
        config_fail_threshold=0,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_invalid_config_fail_threshold(self):
        with pytest.raises(ValueError, match='config fail threshold'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        # invalid
        spark_fetch_trace_timeout=0,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_invalid_fetch_trace_timeout(self):
        with pytest.raises(ValueError, match='spark fetch trace timeout'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        # invalid
        spark_fetch_trace_interval=0,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_invalid_fetch_trace_interval(self):
        with pytest.raises(ValueError, match='spark fetch trace interval'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        # invalid
        spark_exec_timeout_ratio=0,
        spark_history_username='user',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_invalid_exec_timeout_ratio(self):
        with pytest.raises(ValueError, match='spark exec timeout ratio'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        spark_history_username='user',
        # mismatch
        spark_history_password=''
    ))
    def test_check_oa_conf_when_username_password_mismatch_case1(self):
        with pytest.raises(ValueError, match='username and password'):
            check_oa_conf()

    @patch(OA_CONF_PATH, new=SimpleNamespace(
        tuning_retest_times=1,
        config_fail_threshold=1,
        spark_fetch_trace_timeout=10,
        spark_fetch_trace_interval=5,
        spark_exec_timeout_ratio=0.5,
        # mismatch
        spark_history_username='',
        spark_history_password='pass'
    ))
    def test_check_oa_conf_when_username_password_mismatch_case2(self):
        with pytest.raises(ValueError, match='username and password'):
            check_oa_conf()
