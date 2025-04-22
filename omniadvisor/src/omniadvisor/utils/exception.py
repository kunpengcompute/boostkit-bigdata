def add_exception_prefix(e, prefix):
    """
    给异常消息加上前缀
    :param e: 源异常
    :param prefix: 消息前缀
    :return:
    """
    e.args = (f"(omniadvisor) {prefix} {str(e)}",) + e.args[1:]
    return e
