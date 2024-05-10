import utils

SCP_TO_CMD = r'''
expect <<EOF
    set timeout 100
    spawn scp -r {src_file} {username}@{host}:{dest_file}
    expect {{
        "*yes/no*" {{ send "yes\r"; exp_continue }}
        "*ssword*" {{ send "{password}\r"; exp_continue }}
    }}
EOF
'''

REMOTE_EXEC_CMD = r'''
expect <<EOF
    set timeout 100
    spawn ssh {username}@{host} "{cmd}"
    expect {{
        "*yes/no*" {{ send "yes\r"; exp_continue }}
        "*ssword*" {{ send "{password}\r"; exp_continue }}
    }}
EOF
'''

# 远程执行sudo命令，会输两次密码， 第一次是ssh密码，提示符不带sudo字样；第二次是sudo密码，提示符带sudo字样
REMOTE_EXEC_SUDO_CMD = r'''
expect <<EOF
    set timeout 100
    spawn ssh -t {username}@{host} "sudo {cmd}"
    expect {{
        "*yes/no*" {{ send "yes\r"; exp_continue }}
        -re "^(?!.*sudo).*ssword*" {{ send "{password}\r"; exp_continue }}
        "*sudo*ssword*" {{ send "{sudo_password}\r"; exp_continue }}
    }}
EOF
'''


class SSH:
    # 创建ssh连接
    def open(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def scp_to(self, src_file, dest_file):
        cmd = SCP_TO_CMD.format(src_file=src_file, dest_file=dest_file, username=self.username, password=self.password,
                                host=self.host)
        print('start to exec {}'.format(cmd))
        ret = utils.exec_shell_cmd(cmd)
        if ret != 0:
            print('scp {} to {}:{} failed.'.format(src_file, self.host, dest_file))
            exit(ret)
        print('scp {} to {}:{} success.'.format(src_file, self.host, dest_file))

    def remote_exec_cmd(self, cmd):
        remote_cmd = REMOTE_EXEC_CMD.format(username=self.username, password=self.password, host=self.host, cmd=cmd)
        print('start to exec {}'.format(remote_cmd))
        ret = utils.exec_shell_cmd(remote_cmd)
        if ret != 0:
            print('exec {} remote cmd {} failed.'.format(self.host, cmd))
            exit(ret)
        print('exec {} remote cmd {} success.'.format(self.host, cmd))

    def remote_exec_sudo_cmd(self, cmd, sudo_password):
        remote_sudo_cmd = REMOTE_EXEC_SUDO_CMD.format(username=self.username, password=self.password, host=self.host,
                                                      cmd=cmd, sudo_password=sudo_password)
        print('start to exec {}'.format(remote_sudo_cmd))
        ret = utils.exec_shell_cmd(remote_sudo_cmd)
        if ret != 0:
            print('exec {} remote sudo cmd {} failed.'.format(self.host, cmd))
            exit(ret)
        print('exec {} remote sudo cmd {} success.'.format(self.host, cmd))
