# Create your models here.
from django.db import models


class Load(models.Model):
    """
    负载模型
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, null=False)
    exec_master = models.CharField(max_length=20, null=False)
    exec_queue = models.CharField(max_length=20, null=False)
    exec_load = models.CharField(max_length=100, null=False)
    default_config = models.JSONField(null=False)
    best_config = models.JSONField(null=True)
    test_config = models.JSONField(null=True)
    create_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'omniadvisor_load'  # 自定义表名


class Task(models.Model):
    """
    任务模型
    """
    id = models.AutoField(primary_key=True)
    load = models.ForeignKey(Load, on_delete=models.DO_NOTHING, null=False)
    config = models.JSONField(null=False)
    start_time = models.DateTimeField(null=True, auto_now_add=False)
    end_time = models.DateTimeField(null=True, auto_now_add=False)
    exec_status = models.CharField(max_length=10, null=True, choices=[('fail', 'fail'), ('success', 'success')])
    exec_runtime = models.FloatField(null=True)
    exec_trace = models.JSONField(null=True)

    class Meta:
        db_table = 'omniadvisor_task'  # 自定义表名
