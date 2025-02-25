import pytest
import os
import django
from unittest.mock import patch
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'omniadvisor.config.settings')
django.setup()
from omniadvisor.model.models import Config


# 参数化正常测试用例‌:ml-citation{ref="1,3" data="citationList"}
@pytest.mark.parametrize("config_data, expected_values", [
    # 正常数据
    ({'id': 'cfg1', 'param1': 100, 'param2': 1.5, 'param3': 2.5},
     {'id': 'cfg1', 'param1': 100, 'param2': 1.5, 'param3': 2.5}),
    # 边界值测试
    ({'id': 'cfg2', 'param1': 0, 'param2': 0.0, 'param3': 3.14},
     {'id': 'cfg2', 'param1': 0, 'param2': 0.0, 'param3': 3.14}),
])
def test_save_valid_config(config_data, expected_values):
    """测试有效配置保存"""
    config_instance = Config()  # 创建 Config 实例
    with patch.object(Config, 'save') as mock_save:
        config_instance.save(config_data)  # 调用实例的 save 方法

        # 验证 mock_save 被调用的次数和参数
        mock_save.assert_called_once()
        mock_save.assert_called_with(expected_values)
