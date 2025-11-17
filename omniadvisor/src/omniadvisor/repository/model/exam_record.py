from server.models import DatabaseExamRecord
from omniadvisor.repository.model.tuning_record import TuningRecord


class ExamRecord:
    """
    测试记录
    """

    def __init__(self, database_model: DatabaseExamRecord):
        """
        初始化，接收测试记录在数据库中的模型

        :param database_model: 数据库测试记录实例
        """
        self._database_model = database_model
        self._tuning_record = TuningRecord(database_model=self._database_model.tuning_record)

    @property
    def id(self):
        return self._database_model.id

    @property
    def tuning_record(self):
        return self._tuning_record

    @property
    def load(self):
        return self._tuning_record.load

    @property
    def config(self):
        return self._tuning_record.config

    @property
    def start_time(self):
        return self._database_model.start_time

    @property
    def end_time(self):
        return self._database_model.end_time

    @property
    def status(self):
        return self._database_model.status

    @property
    def runtime(self):
        return self._database_model.runtime

    @property
    def application_id(self):
        return self._database_model.application_id

    @property
    def sql_trace(self):
        return self._database_model.sql_trace

    @property
    def stage_trace(self):
        return self._database_model.stage_trace

    @property
    def database_model(self):
        return self._database_model

    class FieldName:
        id = 'id'
        tuning_record = 'tuning_record'
        start_time = 'start_time'
        end_time = 'end_time'
        status = 'status'
        runtime = 'runtime'
        application_id = 'application_id'
        sql_trace = 'sql_trace'
        stage_trace = 'stage_trace'
