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
    tuning_needed = models.BooleanField(default=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['name', 'default_config'], name='load_unique')
        ]
        db_table = 'omniadvisor_load'  # 自定义表名


class DatabaseTuningRecord(models.Model):
    """
    调优记录模型
    """
    TUNING_METHOD_CHOICES = {(method, method.upper()) for method in OA_CONF.TuningMethod.all}

    id = models.AutoField(primary_key=True)
    load = models.ForeignKey(
        DatabaseLoad,
        on_delete=models.DO_NOTHING,
        null=False
    )
    config = models.JSONField(null=False)
    method = models.CharField(
        max_length=10,
        null=False,
        choices=TUNING_METHOD_CHOICES
    )
    method_extend = models.CharField(max_length=20, null=True)
    rounds = models.IntegerField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['load', 'config'], name='exam_record_unique')
        ]
        db_table = 'omniadvisor_tuning_record'  # 自定义表名


class DatabaseExamRecord(models.Model):
    """
    测试记录模型
    """
    TASK_STATUS_CHOICES = [
        (OA_CONF.ExecStatus.success, 'Success'),
        (OA_CONF.ExecStatus.fail, 'Fail'),
        (OA_CONF.ExecStatus.running, 'Running')
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
        default=OA_CONF.ExecStatus.running
    )
    runtime = models.FloatField(null=True)
    trace = models.JSONField(null=True)

    class Meta:
        db_table = 'omniadvisor_exam_record'  # 自定义表名
