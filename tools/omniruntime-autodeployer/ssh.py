import paramiko
import time
import utils

SCP_CMD = r'''#!/bin/bash
expect -c
spawn scp -r {username}@{host}:{source_file} {target_file}
expect {{
\"*assword\" {{set timeout 300; send \"{password}\r\";}}
\"yes/no\" {{send \"yes\r\"; exp_continue;}}
}}
expect eof"'''

SCP_TO_CMD = r'''
expect <<EOF
    set timeout 100
    spawn scp -r {src_file} {username}@{host}:{dest_file}
    expect {
        "yes/no" { send "yes\r"; exp_continue }
        "password" { send "{password}\r"; exp_continue }
    }
EOF
'''

REMOTE_EXEC_CMD = r'''
expect <<EOF
    set timeout 100
    spawn ssh {username}@{host} "{cmd}"
    expect {
        "yes/no" { send "yes\r"; exp_continue }
        "password" { send "{password}\r"; exp_continue }
    }
EOF
'''

class SSH:
    # 创建ssh连接
    def open(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.ssh = paramiko.SSHClient()
        know_host = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(know_host)
        self.ssh.connect(host, port, username, password, allow_agent=False)

    # 关闭连接
    def close(self):
        self.ssh.close()

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
            print('exec remote cmd {} failed.'.format(remote_cmd))
            exit(ret)
        print('exec remote cmd {} success.'.format(remote_cmd))


    # 远程执行命令
    def send_cmd(self, cmd):
        shell = self.ssh.invoke_shell()
        shell.settimeout(1)
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        res, err = stdout.read(), stderr.read()
        result = res + err
        return result.decode("utf-8", "ignore")

    # 远程执行命令，设置超时间（默认为1800s）, 避免卡死
    def send_shell_cmd(self, cmd, timeout=1800):
        shell = self.ssh.invoke_shell()
        shell.settimeout(1)
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        endtime = time.time() + timeout
        while not stdout.channel.eof_received:
            time.sleep(1)
            if time.time() > endtime:
                stdout.channel.close()
                break
        res, err = stdout.read(), stderr.read()
        result = res + err
        return result.decode("utf-8", "ignore")

    # 远程执行expect脚本
    def _exec_expect(self, file_content, file_name):
        self.send_cmd("echo '{}' > {}".format(file_content, file_name))
        self.send_cmd("chmod +x {}".format(file_name))
        result = self.send_cmd("./{}".format(file_name))
        self.send_cmd("rm -rf {}".format(file_name))
        return result

    # 从远程节点拷贝数据到本地
    def scp_from(self, remote, src_file, dest_file):
        expect_installed = self.send_cmd('rpm -qa | grep ^expect > /dev/null 2>&1; echo $?')
        if int(expect_installed) != 0:
            self.send_cmd('yum -y install expect')

        scp_cmd = SCP_CMD.format(username=remote['username'], host=remote['host'], source_file=src_file,
                                 target_file=dest_file, password=remote['password'])
        self._exec_expect(scp_cmd, 'expect_script.sh')

    def is_exists(self, filename, query_cmd='ls'):
        is_exists = self.send_cmd(query_cmd + ' ' + filename + ' > /dev/null 2>&1; echo &?')
        return True if int(is_exists) == 0 else False
