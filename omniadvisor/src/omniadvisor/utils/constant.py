
class OmniAdvisorConf:
    """
    OmniAdvisor常量
    """
    # 任务执行状态
    class TaskStatus:
        success='Success'
        fail='Fail'
        running='Running'
        all=[success, fail, running]

# 构建实例，供调用
OA_CONF = OmniAdvisorConf()
