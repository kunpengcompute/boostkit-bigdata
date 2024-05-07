import shutil
import os

def copy_files(src_dir, dest_dir):
    for f in os.listdir(src_dir):
        src_path = os.path.join(src_dir, f)
        dest_path = os.path.join(dest_dir, f)
        if os.path.isdir(src_path):
            shutil.copytree(src_path, dest_path)
        else:
            shutil.copy2(src_path, dest_path)