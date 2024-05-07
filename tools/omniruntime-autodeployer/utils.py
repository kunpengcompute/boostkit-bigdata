import shutil
import os
import subprocess


def copy_files(src_dir, dest_dir):
    for f in os.listdir(src_dir):
        src_path = os.path.join(src_dir, f)
        dest_path = os.path.join(dest_dir, f)
        if os.path.isdir(src_path):
            shutil.copytree(src_path, dest_path)
        else:
            shutil.copy2(src_path, dest_path)


# 执行shell命令，默认超时时间为600秒
def exec_shell_cmd(cmd, timeout_s=600):
    try:
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        print("exec cmd [{}] success.".format(cmd))
        print("output: {}".format(result.stdout))
        return result.returncode
    except subprocess.TimeoutExpired as e:
        print("exec cmd [{}] timeout after {} seconds.".format(cmd, timeout_s))
        return -1
    except subprocess.CalledProcessError as e:
        print("exec cmd [{}] failed.".format(cmd))
        print("return code: {}, cause: {}".format(e.returncode, e.stderr))
        return e.returncode

# 如果目录以/结尾，则去除/
def trim_dir(dir):
    if dir.endswith('/'):
        return dir[:-1]
    return dir