class ParameterException(Exception):
    """
    由于参数配置不合理引发的异常
    """
    def __init__(self, parameter):
        self.parameter = parameter
        super().__init__(f"Invalid parameters: {parameter}")

    def __str__(self):
        return f"Invalid parameters: {self.parameter}"
