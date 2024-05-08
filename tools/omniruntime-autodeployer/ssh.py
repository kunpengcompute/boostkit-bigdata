import utils

SCP_TO_CMD = r'''
expect <<EOF
    set timeout 100
    spawn scp -r {src_file} {username}@{host}:{dest_file}
    expect {{
        "yes/no" {{ send "yes\r"; exp_continue }}
        "password" {{ send "{password}\r"; exp_continue }}
    }}
EOF
'''

REMOTE_EXEC_CMD = r'''
expect <<EOF
    set timeout 100
    spawn ssh {username}@{host} "{cmd}"
    expect {{
        "yes/no" {{ send "yes\r"; exp_continue }}
        "password" {{ send "{password}\r"; exp_continue }}
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
        cmd = SCP_TO_CMD.format(src_file=src_file, dest_file=dest_file, username=self.username, password=self.password, host=self.host)
        ret = utils.exec_shell_cmd(cmd)
        if ret != 0:
            print('scp {} to {}:{} failed.'.format(src_file, self.host, dest_file))
            exit(ret)
        print('scp {} to {}:{} success.'.format(src_file, self.host, dest_file))

    def remote_exec_cmd(self, cmd):
        remote_cmd = REMOTE_EXEC_CMD.format(username=self.username, password=self.password, host=self.host, cmd=cmd)
        ret = utils.exec_shell_cmd(remote_cmd)
        if ret != 0:
            print('exec {} remote cmd {} failed.'.format(self.host, remote_cmd))
            exit(ret)
        print('exec {} remote cmd {} success.'.format(self.host, cmd))

