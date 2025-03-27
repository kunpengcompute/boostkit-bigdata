# Create your models here.
from django.db import models

from common.constant import OA_CONF


class DatabaseLoad(models.Model):
    """
    负载模型
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, null=False)
    exec_attr = models.JSONField(null=False)
    default_config = models.JSONField(null=False)
    best_config = models.JSONField(null=True)
    test_config = models.JSONField(null=True)
    create_time = models.DateTimeField(auto_now_add=True)
    tuning_needed = models.BooleanField(default=False)

    class Meta:
        db_table = 'omniadvisor_load'  # 自定义表名


class DatabaseTask(models.Model):
    """
    任务模型
    """
    TASK_STATUS_CHOICES = [
        (OA_CONF.TaskStatus.success, 'Success'),
        (OA_CONF.TaskStatus.fail, 'Fail'),
        (OA_CONF.TaskStatus.running, 'Running')
    ]

    id = models.AutoField(primary_key=True)
    load = models.ForeignKey(
        DatabaseLoad,
        on_delete=models.DO_NOTHING,
        null=False
    )
    config = models.JSONField(null=False)
    start_time = models.DateTimeField(null=True, auto_now_add=False)
    end_time = models.DateTimeField(null=True, auto_now_add=False)
    status = models.CharField(
        max_length=10,
        null=True,
        choices=TASK_STATUS_CHOICES,
        default=OA_CONF.TaskStatus.running
    )
    runtime = models.FloatField(null=True)
    trace = models.JSONField(null=True)

    class Meta:
        db_table = 'omniadvisor_task'  # 自定义表名
