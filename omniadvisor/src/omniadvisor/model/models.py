from django.db import models

# create model for database


class Config(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    param1 = models.IntegerField()
    param2 = models.FloatField()
    param3 = models.FloatField()


class Workload(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    name = models.CharField(max_length=100, help_text="script name of the workload")
    app_type = models.CharField(max_length=10, help_text="app type, spark, for example")
    command = models.CharField(max_length=100, help_text="command to execute")
    exec_queue = models.CharField(max_length=100)
    default_config_id = models.ForeignKey(Config, on_delete=models.CASCADE, help_text="config that user provides")
    best_config_id = models.ForeignKey(Config, on_delete=models.CASCADE)
    test_config_id = models.ForeignKey(Config, on_delete=models.CASCADE, help_text="config need to be test")


class Task(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    workload_id = models.ForeignKey(Workload, on_delete=models.CASCADE)
    config_id = models.ForeignKey(Config, on_delete=models.CASCADE)
    start_time = models.TimeField(auto_now_add=True, help_text="start time of task")
    end_time = models.TimeField(auto_now_add=False, help_text="end time of task")
    execute_status = models.CharField(choices=["fail", "success"])
    execute_performance = models.FloatField()
    app_id = models.CharField(help_text="when task finish, app id will be given")


class Performance(models.Model):
    id = models.CharField(max_length=32, unique=True, primary_key=True)
    workload_id = models.ForeignKey(Workload, on_delete=models.CASCADE)
    config_id = models.ForeignKey(Config, on_delete=models.CASCADE)
    has_succeed = models.BooleanField(default=True)
    average_performance = models.FloatField()
    poorest_performance = models.FloatField()
    best_performance = models.FloatField()


