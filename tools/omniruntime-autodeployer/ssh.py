import paramiko
import os
import time

SCP_CMD = r'''#!/bin/bash
expect -c
spawn scp -r {username}@{host}:{source_file} {target_file}
expect {{
\"*assword\" {{set timeout 300; send \"{password}\r\";}}
\"yes/no\" {{send \"yes\r\"; exp_continue;}}
}}
expect eof"'''


class SSH:
    # 创建ssh连接
    def open(self, host, port, username, password):
        self.ssh = paramiko.SSHClient()
        know_host = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(know_host)
        self.ssh.connect(host, port, username, password, allow_agent=False)

    # 关闭连接
    def close(self):
        self.ssh.close()

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

    # 从远端服务器目录获取文件
    def get_file_from_remote(self, remote, remote_file, local_file):
        local_dir = local_file.rsplit('/', 1)[0]
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        transport = paramiko.Transport((remote['host'], remote['port']))
        transport.connect(username=remote['username'], password=remote['password'])
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(remote_file, local_file)
        transport.close()

    def is_exists(self, filename, query_cmd='ls'):
        is_exists = self.send_cmd(query_cmd + ' ' + filename + ' > /dev/null 2>&1; echo &?')
        return True if int(is_exists) == 0 else False
