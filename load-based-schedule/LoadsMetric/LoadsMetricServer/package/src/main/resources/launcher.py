# coding=utf-8
# !/usr/bin/env python

import errno
import os
import platform
import sys
import traceback

from fcntl import flock, LOCK_EX, LOCK_NB
from optparse import OptionParser
from os import O_RDWR, O_CREAT
from os.path import basename, dirname, exists, realpath
from os.path import join as path_join
from signal import SIGTERM, SIGKILL
from time import sleep

COMMANDS = ['start', 'stop', 'restart', 'kill', 'status']
NOT_RUNNING = 3
STATUS_UNKNOWN = 4

class HandleProcess:
    def __init__(self, options):
        self.__path = options.pid_file
        self.__var_path = options.var_dir
        self.__makedir()
        self.__pid_file = self.__open_read_write()
        self.__refresh()
        self.__options = options

    def __makedir(self):
        try:
            os.makedirs(dirname(self.__path), 0o750)
            os.chmod(self.__var_path, 0o750)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def __open_read_write(self):
        return os.fdopen(os.open(self.__path, O_RDWR | O_CREAT, 0o600), 'r+')

    def __try_lock(self):
        try:
            flock(self.__pid_file, LOCK_EX | LOCK_NB)
            return True
        except (IOError, OSError):
            return False

    def __refresh(self):
        self.__locked = self.__try_lock()

    def __clear_pid(self):
        assert self.__locked, 'pid file not locked'
        self.__pid_file.seek(0)
        self.__pid_file.truncate()

    def __write_pid(self, pid):
        self.__clear_pid()
        self.__pid_file.write(str(pid) + '\n')
        self.__pid_file.flush()

    def __alive(self):
        self.__refresh()
        if self.__locked:
            return False

        pid = self.__read_pid()
        try:
            os.kill(pid, 0)
            return True
        except OSError as exception:
            raise Exception('signaling %s pid failed: %s' % (pid, exception))

    def __read_pid(self):
        assert not self.__locked, 'pid file is locked'
        self.__pid_file.seek(0)
        line = self.__pid_file.readline().strip()
        if len(line) == 0:
            raise Exception("%s pid file is empty" % self.__path)

        try:
            pid = int(line)
        except ValueError:
            raise Exception("%s pid file contains garbage: %s"
                            % (self.__path, line))
        if pid <= 0:
            raise Exception("%s pid file contains an invalid pid: %s"
                            % (self.__path, pid))
        return pid

    def __terminate(self, signal, message):
        if not self.__alive():
            print('Not Running')
            return

        pid = self.__read_pid()

        while True:
            try:
                os.kill(pid, signal)
            except OSError as exception:
                if exception.errno != errno.ESRCH:
                    raise Exception('Signaling %s pid failed: %s'
                                    % (pid, exception))

            if not self.__alive():
                self.__clear_pid()
                break

            sleep(0.1)

        print('%s %s' % (message, pid))

    def __build_java_execution(self):
        if not exists(self.__options.server_config_path) and not exists(self.__options.client_config_path):
            raise Exception('Config file is missing: %s or %s'
                            % (self.__options.server_config_path,self.__options.client_config_path))
        if not exists(self.__options.jvm_config):
            raise Exception('JVM config file is missing: %s'
                            % self.__options.jvm_config)
        if not exists(self.__options.launcher_config):
            raise Exception('Launcher config file is missing: %s'
                            % self.__options.launcher_config)
        if not exists(self.__options.log_config):
            raise Exception('Log levels file is missing: %s'
                            % self.__options.log_levels)

        jvm_properties = load_lines(self.__options.jvm_config)
        launcher_properties = load_properties(self.__options.launcher_config)
        properties = self.__options.properties.copy()
        properties['logback.configurationFile'] = self.__options.log_config
        properties['logging.config'] = self.__options.log_config
        properties['config'] = self.__options.node_config
        system_properties = ['-D%s=%s' % i for i in properties.items()]
        classpath = ":".join(
            [path_join(self.__options.install_path, 'lib', '*'), self.__options.install_path])

        try:
            main_class = launcher_properties['main-class']
        except KeyError:
            raise Exception("Launcher config is missing 'main-class' property")

        command = ['java', '-cp', classpath]
        command += jvm_properties + system_properties
        command += ['-DloadsMetricServerConfig=%s' % self.__options.server_config_path]
        command += ['-DworkersConfig=%s' % self.__options.workers_config_path]
        command += [main_class]

        if self.__options.verbose:
            print(command)
            print("")

        env = os.environ.copy()
        env['SPRING_CONFIG_LOCATION'] = 'file:%s' %self.__options.server_config_path

        process_name = launcher_properties.get('process-name', '')
        if len(process_name) > 0:
            system = platform.system() + '-' + platform.machine()
            shim = path_join(self.__options.install_path,
                             'bin', 'procname', system, 'libprocname.so')
            if exists(shim):
                env['LD_PRELOAD'] = (env.get('LD_PRELOAD', '')
                                     + ':' + shim).strip()
                env['PROCNAME'] = process_name

        return command, env

    def status(self):
        """show the process pid"""
        if not self.__alive():
            print('Not running')
            sys.exit(NOT_RUNNING)
        print('Running')

    def start(self):
        """start the process"""
        if self.__alive():
            print('Already running, the pid is %s' % self.__read_pid())
            return

        args, env = self.__build_java_execution()

        pid = os.fork()
        if pid > 0:
            self.__write_pid(pid)
            print('Started,  pid is %s' % pid)
            return

        if hasattr(os, "set_inheritable"):
            os.set_inheritable(self.__pid_file.fileno(), True)

        os.setsid()

        os.execvpe(args[0], args, env)

    def stop(self):
        """stop the process"""
        self.__terminate(SIGTERM, 'Stopped')

    def restart(self):
        """restart the process"""
        self.stop()
        self.start()

    def kill(self):
        """kill the process"""
        self.__terminate(SIGKILL, 'Killed')


class Options:
    pass


def generate_parser():
    """create the parser"""
    commands_description = "Commands" + ", ".join(COMMANDS)
    parser = OptionParser(prog='launcher',
                          usage='usage: %prog [options] command',
                          description=commands_description)
    parser.add_option('--jvm-config', metavar='FILE',
                      help='The defaults for ETC_DIR/jvm.config')
    parser.add_option('--logback-config', metavar='FILE',
                      help='The defaults for INSTALL_PATH/etc/logback.xml')
    parser.add_option('-v', '--verbose', action='store_true',
                      default=False, help='Run with verbose mode')
    parser.add_option('-D', action='append', metavar='NAME=VALUE',
                      dest='properties', help='Append a Java system property')
    return parser

def load_properties(path):
    """load properties from the file"""
    properties = {}
    for line in load_lines(path):
        key, value = line.split('=', 1)
        properties[key.strip()] = value.strip()
    return properties


def load_lines(path):
    """load lines from the file"""
    lines = []
    for line in open(path, 'r').readlines():
        line = line.strip()
        if len(line) > 0 and not line.startswith('#'):
            lines.append(line)
    return lines


def parse_properties(parser, args):
    """parse properties"""
    properties = {}
    for arg in args:
        if '=' not in arg:
            parser.error('%s : this property is not adequate' % arg)
        name, value = [i.strip() for i in arg.split('=', 1)]
        if name == 'config':
            parser.error('use --config, or  -D option')
        properties[name] = value
    return properties


def handle_command(command, options):
    """handle commands with options"""
    handle = HandleProcess(options)
    if command == 'start':
        handle.start()
    elif command == 'stop':
        handle.stop()
    elif command == 'restart':
        handle.restart()
    elif command == 'kill':
        handle.kill()
    elif command == 'status':
        handle.status()
    else:
        raise AssertionError('Unhandled command: ' + command)


def find_install_path(path):
    """find canonical parent of bin/launcher.py"""
    if basename(path) != 'launcher.py':
        raise Exception("file %s should be launcher.py not %s"
                        % (path, basename(path)))
    parent_name = realpath(dirname(path))
    if basename(parent_name) != 'bin':
        raise Exception("file %s directory should be bin not %s"
                        % (path, basename(parent_name)))
    return dirname(parent_name)


def main():
    parser = generate_parser()
    (options, args) = parser.parse_args()

    if len(args) != 1:
        if len(args) == 0:
            parser.error('no command name to parser ')
        else:
            parser.error('there are too many arguments to parse')
    command = args[0]

    if command not in COMMANDS:
        parser.error('this %s command unsupported' % command)

    try:
        install_path = find_install_path(sys.argv[0])
    except Exception as exception:
        print('ERROR: %s' % exception)
        sys.exit(STATUS_UNKNOWN)

    os.chdir(install_path)

    opt = Options()
    opt.install_path = install_path
    opt.etc_dir = realpath(path_join(opt.install_path, 'etc'))
    opt.node_config = realpath(path_join(opt.etc_dir,
                                         'airliftconfig.properties'))
    opt.jvm_config = realpath(options.jvm_config
                              or path_join(opt.etc_dir, 'jvm.config'))
    opt.server_config_path = realpath(path_join(opt.etc_dir, 'application.properties'))
    opt.workers_config_path = realpath(path_join(opt.etc_dir, 'workers'))
    opt.verbose = options.verbose
    opt.launcher_config = realpath(path_join(opt.install_path,
                                             'bin/launcher.properties'))
    opt.var_dir = realpath(path_join(opt.install_path, 'var'))
    opt.pid_file = realpath(path_join(opt.install_path,
                                      'var/run/launcher.pid'))
    opt.log_config = realpath(options.logback_config
                              or path_join(opt.etc_dir, 'logback.xml'))
    opt.properties = parse_properties(parser, options.properties or {})

    node_properties = {}
    if exists(opt.node_config):
        node_properties = load_properties(opt.node_config)

    for key, value in node_properties.items():
        if key not in opt.properties:
            opt.properties[key] = value

    if opt.verbose:
        for i in sorted(vars(opt)):
            print("%-15s = %s" % (i, getattr(opt, i)))
        print("")

    try:
        handle_command(command, opt)
    except SystemExit:
        raise
    except Exception as exception:
        if opt.verbose:
            traceback.print_exc()
        else:
            print('ERROR: %s' % exception)
        sys.exit(STATUS_UNKNOWN)


if __name__ == '__main__':
    main()