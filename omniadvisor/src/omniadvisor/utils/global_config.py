import os


def get_project_dir():
    current_file = __file__
    current_file_path = os.path.abspath(current_file)
    project_path = current_file_path
    level = 2
    for _ in range(level):
        project_path = os.path.dirname(project_path)
    return project_path


def load_constraint():
    """
    加载配置约束文件
    """
    constraint_file_path = ''
    return constraint_file_path


# 根目录
project_dir = get_project_dir()

# 资源约束文件
constraint_config = load_constraint()

# 复测次数
retest_times = 3

# 最大失败配置个数
max_failed_configs = 5
