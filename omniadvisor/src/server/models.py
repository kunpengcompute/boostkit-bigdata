# Create your models here.
from django.db import models

from common.constant import OA_CONF


class DatabaseLoad(models.Model):
    """
    负载模型
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=180, null=True)
    exec_attr = models.JSONField(null=False)
    default_config = models.JSONField(null=False)
    best_config = models.JSONField(null=True)
    test_config = models.JSONField(null=True)
    create_time = models.DateTimeField(auto_now_add=True)
    tuning_needed = models.BooleanField(default=True)
    hash_value = models.CharField(max_length=64, null=False, unique=True)
    backend_retest_enable = models.BooleanField(default=False)

    class Meta:
        app_label = 'server'
        db_table = 'omniadvisor_load'  # 自定义表名


class DatabaseTuningRecord(models.Model):
    """
    调优记录模型
    """
    _TUNING_METHOD_CHOICES = {(method, method.upper()) for method in OA_CONF.TuningMethod.all}

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
        choices=_TUNING_METHOD_CHOICES
    )
    method_extend = models.CharField(max_length=50, null=True)
    rounds = models.IntegerField(null=True)

    class Meta:
        app_label = 'server'
        constraints = [
            models.UniqueConstraint(fields=['load', 'config'], name='tuning_record_unique')
        ]
        db_table = 'omniadvisor_tuning_record'  # 自定义表名


class DatabaseExamRecord(models.Model):
    """
    测试记录模型
    """
    _TASK_STATUS_CHOICES = [
        (OA_CONF.ExecStatus.success, 'Success'),
        (OA_CONF.ExecStatus.fail, 'Fail'),
        (OA_CONF.ExecStatus.running, 'Running')
    ]

    id = models.AutoField(primary_key=True)
    tuning_record = models.ForeignKey(
        DatabaseTuningRecord,
        on_delete=models.DO_NOTHING,
        null=False
    )
    start_time = models.DateTimeField(null=True, auto_now_add=False)
    end_time = models.DateTimeField(null=True, auto_now_add=False)
    status = models.CharField(
        max_length=10,
        null=True,
        choices=_TASK_STATUS_CHOICES,
        default=OA_CONF.ExecStatus.running
    )
    runtime = models.FloatField(null=True)
    application_id = models.CharField(max_length=50, null=True)
    sql_trace = models.TextField()
    stage_trace = models.TextField()

    class Meta:
        app_label = 'server'
        db_table = 'omniadvisor_exam_record'  # 自定义表名
