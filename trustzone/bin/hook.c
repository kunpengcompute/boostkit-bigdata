#define _GNU_SOURCE
#define LOG_PATH "/home/xxx/new_hook/posix.log"

#include <stdio.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>

void print_log(const char *fmt, ...)
{
    static FILE *fd = NULL;
    
    if (fd == NULL) {
        if (fd == NULL){
            fd = fopen(LOG_PATH, "a+");
            if (fd == NULL) {
                fprintf(stderr, "open log file failed.\n");
                return;
            }
        }
    }

    va_list args;
    va_start(args, fmt);
    if (fd != NULL)
        vfprintf(fd, fmt, args);
    va_end(args);
}
#define LOG_FUNC(fmt, args...) print_log("[%s]\t\t" fmt "\n", ##args)

typedef void *(*MMAP)(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
{
    static MMAP old_func = NULL;
    if (fd >= 0) {
        char buf[1024] = {'\0'};
        char file_path[1024] = {'\0'};
        snprintf(buf,sizeof(buf), "/proc/self/fd/%d", fd);
        readlink(buf,file_path,sizeof(file_path)-1);
        LOG_FUNC("{mmap fd is: %d, file path is: %s\n}", __func__, fd, file_path);
    }
    old_func = (MMAP)dlsym(RTLD_NEXT, "mmap");
    return old_func(addr, length, prot, flags, fd, offset);
}

typedef ssize_t (*SENDFILE)(int out_fd, int in_fd, off_t *ofs, size_t count);

ssize_t sendfile(int out_fd, int in_fd, off_t *ofs, size_t count)
{
    static SENDFILE old_func = NULL;
    LOG_FUNC("{ +++++++ sendfile ++++++++ }\n", __func__);
    if (in_fd >= 0) {
        char buf[1024] = {'\0'};
        char file_path[1024] = {'\0'};
        snprintf(buf,sizeof(buf), "/proc/self/fd/%d", in_fd);
        readlink(buf,file_path,sizeof(file_path)-1);
        LOG_FUNC("{mmap in_fd is: %d, file path is: %s\n}", __func__, in_fd, file_path);
    }

    if (out_fd >= 0) {
        char buf2[1024] = {'\0'};
        char file_path2[1024] = {'\0'};
        snprintf(buf2,sizeof(buf2), "/proc/self/fd/%d", in_fd);
        readlink(buf2,file_path2,sizeof(file_path2)-1);
        LOG_FUNC("{mmap out_fd is: %d, file path is: %s\n}", __func__, out_fd, file_path2);
    }

    old_func = (MMAP)dlsym(RTLD_NEXT, "sendfile");
    return old_func(out_fd, in_fd, ofs, count);
}

typedef int(*BIND)(int, __CONST_SOCKADDR_ARG, socklen_t);
typedef int(*SOCKET)(int, int, int);
typedef int(*ACCEPT)(int, __SOCKADDR_ARG, socklen_t *);
typedef int (*CONNECT)(int fd, const struct sockaddr *addr, socklen_t len);
typedef int (*LISTEN)(int __fd, int __n);
typedef ssize_t (*SEND)(int __fd, const void *__buf, size_t __n, int __flags);
typedef ssize_t (*SENDMSG)(int __fd, const struct msghdr *__message, int __flags);
typedef ssize_t (*SENDTO)(int, const void *, size_t, int, __CONST_SOCKADDR_ARG, socklen_t);
typedef ssize_t (*RECV)(int __fd, void *__buf, size_t __n, int __flags);
typedef ssize_t (*RECVFROM) (int __fd, void *__restrict __buf, size_t __n, int __flags, __SOCKADDR_ARG __addr, socklen_t *__restrict __addr_len);
typedef ssize_t (*RECVMSG)(int __fd, struct msghdr *__message, int __flags);
typedef int (*SHUTDOWN) (int __fd, int __how);
typedef int (*SETSOCKOPT) (int __fd, int __level, int __optname, const void *__optval, socklen_t __optlen);
typedef int (*GETSOCKOPT)(int __fd, int __level, int __optname, void *__restrict __optval, socklen_t *__restrict __optlen);
typedef int (*SOCKETPAIR)(int __domain, int __type, int __protocol, int __fds[2]);
typedef int (*GETSOCKNAME)(int __fd, __SOCKADDR_ARG __addr, socklen_t *__restrict __len);
typedef int (*GETPEERNAME)(int __fd, __SOCKADDR_ARG __addr, socklen_t *__restrict __len);
typedef int (*SOCKATMARK)(int __fd);

int bind(int __fd,  const struct sockaddr * __addr, socklen_t __len)
{
    static BIND old_bind = NULL;

    LOG_FUNC("{ family: %u }", __func__, __addr->sa_family);
    old_bind = (BIND)dlsym(RTLD_NEXT, "bind");
    
    return old_bind(__fd, __addr, __len);
}

int accept(int __fd, struct sockaddr * __addr, socklen_t *__restrict __addr_len)
{
    static ACCEPT old_accept = NULL;

    LOG_FUNC("{ family: %u }", __func__, __addr->sa_family);
    old_accept = (ACCEPT)dlsym(RTLD_NEXT, "accept");
    
    return old_accept(__fd, __addr, __addr_len);
}

int socket(int __domain, int __type, int __protocol)
{
    static SOCKET old_socket = NULL;
    int ret = 0;

    LOG_FUNC("{ domain: %d, type: %o, protocol: %d }", __func__, __domain, __type, __protocol);
    old_socket = (SOCKET)dlsym(RTLD_NEXT, "socket");

    return old_socket(__domain, __type, __protocol);
}

int connect(int fd, const struct sockaddr *addr, socklen_t len)
{
    static CONNECT old_connect = NULL;

    LOG_FUNC("{ family: %u }", __func__, addr->sa_family);
    old_connect = (CONNECT)dlsym(RTLD_NEXT, "connect");

    return old_connect(fd, addr, len);
}

int listen (int __fd, int __n)
{
    static LISTEN old_listen = NULL;

    LOG_FUNC("{ fd: %d, backlog: %d }", __func__, __fd, __n);
    old_listen = (LISTEN)dlsym(RTLD_NEXT, "listen");

    return old_listen(__fd, __n);
}

ssize_t send(int __fd, const void *__buf, size_t __n, int __flags)
{
    static SEND old_send = NULL;

    LOG_FUNC("{ size: %lu, flag: %d }", __func__, __n, __flags);
    old_send = (SEND)dlsym(RTLD_NEXT, "send");
    
    return old_send(__fd, __buf, __n, __flags);
}

ssize_t sendmsg (int __fd, const struct msghdr *__message, int __flags)
{
    static SENDMSG old_sendmsg = NULL;

    LOG_FUNC("", __func__);
    old_sendmsg = (SENDMSG)dlsym(RTLD_NEXT, "sendmsg");

    return old_sendmsg(__fd, __message, __flags);
}

ssize_t sendto (int __fd, const void *__buf, size_t __n, int __flags, const struct sockaddr * __addr, socklen_t __addr_len)
{
    static SENDTO old_sendto = NULL;

    LOG_FUNC("{ flags: %u, family: %u}", __func__, __flags, __addr->sa_family);
    old_sendto = (SENDTO)dlsym(RTLD_NEXT, "sendto");

    return old_sendto(__fd, __buf, __n, __flags, __addr, __addr_len);
}

ssize_t recv (int __fd, void *__buf, size_t __n, int __flags)
{
    static RECV old_recv = NULL;

    LOG_FUNC("{ size: %u, flags: %d }", __func__, __n, __flags);
    old_recv = (RECV)dlsym(RTLD_NEXT, "recv");

    return old_recv(__fd, __buf, __n, __flags);
}

ssize_t recvfrom (int __fd, void *__restrict __buf, size_t __n, \
    int __flags, struct sockaddr * __addr, socklen_t *__restrict __addr_len)
{
    static RECVFROM old_recvfrom = NULL;

    LOG_FUNC("{ size: %u, flags: %d, family: %u }", __func__, __n, __flags ,__addr->sa_family);
    old_recvfrom = (RECVFROM)dlsym(RTLD_NEXT, "recvfrom");

    return old_recvfrom(__fd, __buf, __n, __flags, __addr, __addr_len);
}

ssize_t recvmsg (int __fd, struct msghdr *__message, int __flags)
{
    static RECVMSG old_recvmsg = NULL;

    LOG_FUNC("{ flags: %d }", __func__, __flags);
    old_recvmsg = (RECVMSG)dlsym(RTLD_NEXT, "recvmsg");

    return old_recvmsg(__fd, __message, __flags);
}


int shutdown (int __fd, int __how)
{
    static SHUTDOWN old_shutdown = NULL;
    LOG_FUNC("{ fd: %d, how: %d }", __func__, __fd, __how);
    old_shutdown = (SHUTDOWN)dlsym(RTLD_NEXT, "shutdown");

    return old_shutdown(__fd, __how);
}


int setsockopt (int __fd, int __level, int __optname,
               const void *__optval, socklen_t __optlen)
{
    static SETSOCKOPT old_setsockopt = NULL;

    LOG_FUNC("{ fd: %d, level: %d, optname: %d, optlen: %u }", __func__, __fd, __level, __optname, __optlen);
    old_setsockopt = (SETSOCKOPT)dlsym(RTLD_NEXT, "setsockopt");

    return old_setsockopt(__fd, __level, __optname, __optval, __optlen);
}

int getsockopt (int __fd, int __level, int __optname, void *__restrict __optval, \
    socklen_t *__restrict __optlen)
{
    static GETSOCKOPT old_getsockopt = NULL;

    LOG_FUNC("{ fd: %d, level: %d, optname: %d, optlen: %u }", __func__, __fd, __level, __optname, __optlen);
    old_getsockopt = (GETSOCKOPT)dlsym(RTLD_NEXT, "getsockopt");

    return old_getsockopt(__fd, __level, __optname, __optval, __optlen);
}


int socketpair (int __domain, int __type, int __protocol,
               int __fds[2])
{
    static SOCKETPAIR old_socketpair = NULL;

    LOG_FUNC("{ domain: %d, type: %d, protocol: %d }", __func__, __domain, __type, __protocol);
    old_socketpair = (SOCKETPAIR)dlsym(RTLD_NEXT, "socketpair");

    return old_socketpair(__domain, __type, __protocol, __fds);
}

int getsockname (int __fd, struct sockaddr * __addr, socklen_t *__restrict __len)
{
    static GETSOCKNAME old_getsockname = NULL;

    LOG_FUNC("{ family: %u }", __func__, __addr->sa_family);
    old_getsockname = (GETSOCKNAME)dlsym(RTLD_NEXT, "getsockname");

    return old_getsockname(__fd, __addr, __len);
}

int getpeername (int __fd, struct sockaddr * __addr, socklen_t *__restrict __len)
{
    static GETPEERNAME old_getpeername = NULL;

    LOG_FUNC("{ family: %u }", __func__, __addr->sa_family);
    old_getpeername = (GETPEERNAME)dlsym(RTLD_NEXT, "getpeername");
    
    return old_getpeername(__fd, __addr, __len);
}

int sockatmark (int __fd)
{
    static SOCKATMARK old_sockatmark = NULL;

    LOG_FUNC("", __func__);
    old_sockatmark = (SOCKATMARK)dlsym(RTLD_NEXT, "sockatmark");

    return old_sockatmark(__fd);
}


typedef int(*DUP)(int oldfd);
typedef int(*DUP2)(int oldfd, int newfd);
int dup(int oldfd)
{
    static DUP old_func = NULL;
    LOG_FUNC("{ oldfd: %d }", __func__, oldfd);
    old_func = (DUP)dlsym(RTLD_NEXT, "dup");
    return old_func(oldfd);
}
int dup2(int oldfd, int newfd)
{
    static DUP2 old_func = NULL;
    LOG_FUNC("{ oldfd: %d, newfd: %d }", __func__, oldfd, newfd);
    old_func = (DUP2)dlsym(RTLD_NEXT, "dup2");
    return old_func(oldfd, newfd);
}

typedef int(*CHMOD)(const char *pathname, mode_t mode);
typedef int(*FCHMOD)(int fd, mode_t mode);
int chmod(const char *pathname, mode_t mode)
{
    static CHMOD old_func = NULL;
    LOG_FUNC("{ pathname: %s, mode: %d }", __func__, pathname, mode);
    old_func = (CHMOD)dlsym(RTLD_NEXT, "chmod");
    return old_func(pathname, mode);
}
int fchmod(int fd, mode_t mode)
{
    static FCHMOD old_func = NULL;
    LOG_FUNC("{ fd: %d, mode: %d }", __func__, fd, mode);
    old_func = (FCHMOD)dlsym(RTLD_NEXT, "fchmod");
    return old_func(fd, mode);
}

typedef int(*CHOWN)(const char *pathname, uid_t owner, gid_t group);
typedef int(*FCHOWN)(int fd, uid_t owner, gid_t group);
typedef int(*LCHOWN)(const char *pathname, uid_t owner, gid_t group);
int chown(const char *pathname, uid_t owner, gid_t group){
    static CHOWN old_func = NULL;
    LOG_FUNC("{ pathname: %s, owner: %u }", __func__, pathname, owner);
    old_func = (CHOWN)dlsym(RTLD_NEXT, "chown");
    return old_func(pathname, owner, group);
}
int fchown(int fd, uid_t owner, gid_t group){
    static FCHOWN old_func = NULL;
    LOG_FUNC("{ fd: %d, owner: %d }", __func__, fd, owner);
    old_func = (FCHOWN)dlsym(RTLD_NEXT, "fchown");
    return old_func(fd, owner, group);
}
int lchown(const char *pathname, uid_t owner, gid_t group){
    static LCHOWN old_func = NULL;
    LOG_FUNC("{ pathname: %s, owner: %u }", __func__, pathname, owner);
    old_func = (LCHOWN)dlsym(RTLD_NEXT, "lchown");
    return old_func(pathname, owner, group);
}


typedef long (*FPATHCONF)(int fd, int name);
typedef long (*PATHCONF)(const char *path, int name);
long fpathconf(int fd, int name){
    static FPATHCONF old_func = NULL;
    LOG_FUNC("{ fd: %d, name: %d }", __func__, fd, name);
    old_func = (FPATHCONF)dlsym(RTLD_NEXT, "fpathconf");
    return old_func(fd, name);
}
long pathconf(const char *path, int name){
    static PATHCONF old_func = NULL;
    LOG_FUNC("{ path: %s, name: %d }", __func__, path, name);
    old_func = (PATHCONF)dlsym(RTLD_NEXT, "pathconf");
    return old_func(path, name);
}

#include <fcntl.h>
typedef int (*FCNTL)(int fd, int cmd, ...);
int fcntl(int fd, int cmd, ...){
    static FCNTL old_func = NULL;
    LOG_FUNC("{ fd: %d, cmd: %d }", __func__, fd, cmd);
    old_func = (FCNTL)dlsym(RTLD_NEXT, "fcntl");
    
    va_list args;
    va_start(args, cmd);
    int ret = old_func(fd, cmd, args);
    va_end(args);
    return ret;
}

typedef int (*STAT)(const char *pathname, struct stat *statbuf);
typedef int (*FSTAT)(int fd, struct stat *statbuf);
typedef int (*LSTAT)(const char *pathname, struct stat *statbuf);
int stat(const char *pathname, struct stat *statbuf){
    static STAT old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (STAT)dlsym(RTLD_NEXT, "stat");
    return old_func(pathname, statbuf);
}
int fstat(int fd, struct stat *statbuf){
    static FSTAT old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (FSTAT)dlsym(RTLD_NEXT, "fstat");
    return old_func(fd, statbuf);
}
int lstat(const char *pathname, struct stat *statbuf){
    static LSTAT old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (LSTAT)dlsym(RTLD_NEXT, "lstat");
    return old_func(pathname, statbuf);
}

typedef int (*TRUNCATE)(const char *path, off_t length);
typedef int (*FTRUNCATE)(int fd, off_t length);
int truncate(const char *path, off_t length){
    static TRUNCATE old_func = NULL;
    LOG_FUNC("{ path: %s length: %u }", __func__, path, length);
    old_func = (TRUNCATE)dlsym(RTLD_NEXT, "truncate");
    return old_func(path, length);
}
int ftruncate(int fd, off_t length){
    static FTRUNCATE old_func = NULL;
    LOG_FUNC("{ fd: %d length: %u }", __func__, fd, length);
    old_func = (FTRUNCATE)dlsym(RTLD_NEXT, "ftruncate");
    return old_func(fd, length);
}

typedef void *(*MMAP)(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
typedef int (*MUNMAP)(void *addr, size_t length);
void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset){
    static MMAP old_func = NULL;
    LOG_FUNC("{ addr: %p length: %lu }", __func__, addr, length);
    old_func = (MMAP)dlsym(RTLD_NEXT, "mmap");
    return old_func(addr, length, prot, flags, fd, offset);
}
int munmap(void *addr, size_t length){
    static MUNMAP old_func = NULL;
    LOG_FUNC("{ addr: %p length: %lu }", __func__, addr, length);
    old_func = (MUNMAP)dlsym(RTLD_NEXT, "munmap");
    return old_func(addr, length);
}

typedef int (*SHM_OPEN)(const char *name, int oflag, mode_t mode);
typedef int (*SHM_UNLINK)(const char *name);
int shm_open(const char *name, int oflag, mode_t mode){
    static SHM_OPEN old_func = NULL;
    LOG_FUNC("{ name: %s  }", __func__, name);
    old_func = (SHM_OPEN)dlsym(RTLD_NEXT, "shm_open");
    return old_func(name, oflag, mode);
}
int shm_unlink(const char *name){
    static SHM_UNLINK old_func = NULL;
    LOG_FUNC("{ name: %s  }", __func__, name);
    old_func = (SHM_UNLINK)dlsym(RTLD_NEXT, "shm_unlink");
    return old_func(name);
}


typedef int (*ACCESS)(const char *pathname, int mode);
int access(const char *pathname, int mode){
    static ACCESS old_func = NULL;
    LOG_FUNC("{ pathname: %s  }", __func__, pathname);
    old_func = (ACCESS)dlsym(RTLD_NEXT, "access");
    return old_func(pathname, mode);
}

typedef int (*CHDIR)(const char *path);
typedef int (*FCHDIR)(int fd);
int chdir(const char *path){
    static CHDIR old_func = NULL;
    LOG_FUNC("{ pathname: %s  }", __func__, path);
    old_func = (CHDIR)dlsym(RTLD_NEXT, "chdir");
    return old_func(path);
}
int fchdir(int fd){
    static FCHDIR old_func = NULL;
    LOG_FUNC("{ fd: %d  }", __func__, fd);
    old_func = (FCHDIR)dlsym(RTLD_NEXT, "fchdir");
    return old_func(fd);
}

typedef int (*CHROOT)(const char *path);
int chroot(const char *path){
    static CHROOT old_func = NULL;
    LOG_FUNC("{ pathname: %s  }", __func__, path);
    old_func = (CHROOT)dlsym(RTLD_NEXT, "chroot");
    return old_func(path);
}

typedef int (*FLOCK)(int fd, int operation);
int flock(int fd, int operation){
    static FLOCK old_func = NULL;
    LOG_FUNC("{ fd: %d, operation: %d }", __func__, fd, operation);
    old_func = (FLOCK)dlsym(RTLD_NEXT, "flock");
    return old_func(fd, operation);
}

typedef int (*FSYNC)(int fd);
typedef int (*FDATASYNC)(int fd);
int fsync(int fd){
    static FSYNC old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (FSYNC)dlsym(RTLD_NEXT, "fsync");
    return old_func(fd);
}
int fdatasync(int fd){
    static FDATASYNC old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (FDATASYNC)dlsym(RTLD_NEXT, "fdatasync");
    return old_func(fd);
}

typedef int (*GETDENTS)(unsigned int fd, struct linux_dirent *dirp, unsigned int count);
typedef int (*GETDENTS64)(unsigned int fd, struct linux_dirent64 *dirp, unsigned int count);
int getdents(unsigned int fd, struct linux_dirent *dirp, unsigned int count){
    static GETDENTS old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (GETDENTS)dlsym(RTLD_NEXT, "getdents");
    return old_func(fd, dirp, count);
}
int getdents64(unsigned int fd, struct linux_dirent64 *dirp, unsigned int count){
    static GETDENTS64 old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (GETDENTS64)dlsym(RTLD_NEXT, "getdents64");
    return old_func(fd, dirp, count);
}

#include <dirent.h>
typedef ssize_t (*GETDIRENTRIES)(int fd, char *buf, size_t nbytes , off_t *basep);
ssize_t getdirentries(int fd, char *buf, size_t nbytes , off_t *basep){
    static GETDIRENTRIES old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (GETDIRENTRIES)dlsym(RTLD_NEXT, "getdirentries");
    return old_func(fd, buf, nbytes, basep);
}

#include <sys/ioctl.h>
int ioctl(int fd, unsigned long request, ...);
int ioctl(int fd, unsigned long request, ...){
}

typedef int (*LINK)(const char *oldpath, const char *newpath);
int link(const char *oldpath, const char *newpath){
    static LINK old_func = NULL;
    LOG_FUNC("{ oldpath: %s, newpath: %s }", __func__, oldpath, newpath);
    old_func = (LINK)dlsym(RTLD_NEXT, "link");
    return old_func(oldpath, newpath);
}

typedef off_t (*LSEEK)(int fd, off_t offset, int whence);
off_t lseek(int fd, off_t offset, int whence){
    static LSEEK old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (LSEEK)dlsym(RTLD_NEXT, "lseek");
    return old_func(fd, offset, whence);
}


typedef int (*MKDIR)(const char *pathname, mode_t mode);
int mkdir(const char *pathname, mode_t mode){
    static MKDIR old_func = NULL;
    LOG_FUNC("{ pathname: %s }", __func__, pathname);
    old_func = (MKDIR)dlsym(RTLD_NEXT, "mkdir");
    return old_func(pathname, mode);
}

typedef int (*MKFIFO)(const char *pathname, mode_t mode);
int mkfifo(const char *pathname, mode_t mode){
    static MKFIFO old_func = NULL;
    LOG_FUNC("{ pathname: %s }", __func__, pathname);
    old_func = (MKFIFO)dlsym(RTLD_NEXT, "mkfifo");
    return old_func(pathname, mode);
}

typedef int (*MKNOD)(const char *pathname, mode_t mode, dev_t dev);
int mknod(const char *pathname, mode_t mode, dev_t dev){
    static MKNOD old_func = NULL;
    LOG_FUNC("{ pathname: %s }", __func__, pathname);
    old_func = (MKNOD)dlsym(RTLD_NEXT, "mknod");
    return old_func(pathname, mode, dev);
}

#include <sys/mount.h>
typedef int (*MOUNT)(const char *source, const char *target, const char *filesystemtype, unsigned long mountflags, const void *data);
int mount(const char *source, const char *target, const char *filesystemtype, unsigned long mountflags, const void *data){
    static MOUNT old_func = NULL;
    LOG_FUNC("{ source: %s, target: %s }", __func__, source, target);
    old_func = (MOUNT)dlsym(RTLD_NEXT, "mount");
    return old_func(source, target, filesystemtype, mountflags, data);
}
typedef int (*UMOUNT)(const char *target);
typedef int (*UMOUNT2)(const char *target, int flags);
int umount(const char *target){
    static UMOUNT old_func = NULL;
    LOG_FUNC("{ target: %s }", __func__, target);
    old_func = (UMOUNT)dlsym(RTLD_NEXT, "umount");
    return old_func(target);
}
int umount2(const char *target, int flags){
    static UMOUNT2 old_func = NULL;
    LOG_FUNC("{ target: %s }", __func__, target);
    old_func = (UMOUNT2)dlsym(RTLD_NEXT, "umount2");
    return old_func(target, flags);
}

typedef int (*OPEN)(const char *pathname, int flags, mode_t mode);
int open(const char *pathname, int flags, mode_t mode){
    static OPEN old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (OPEN)dlsym(RTLD_NEXT, "open");
    return old_func(pathname, flags, mode);
}

typedef ssize_t (*READ)(int fd, void *buf, size_t count);
ssize_t read(int fd, void *buf, size_t count){
    static READ old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (READ)dlsym(RTLD_NEXT, "read");
    return old_func(fd, buf, count);
}

typedef ssize_t (*READLINK)(const char *pathname, char *buf, size_t bufsiz);
ssize_t readlink(const char *pathname, char *buf, size_t bufsiz){
    static READLINK old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (READLINK)dlsym(RTLD_NEXT, "readlink");
    return old_func(pathname, buf, bufsiz);
}

typedef int (*RENAME)(const char *oldpath, const char *newpath);
int rename(const char *oldpath, const char *newpath){
    static RENAME old_func = NULL;
    LOG_FUNC("{ old: %s new: %s }", __func__, oldpath, newpath);
    old_func = (RENAME)dlsym(RTLD_NEXT, "rename");
    return old_func(oldpath, newpath);
}

typedef int (*RMDIR)(const char *pathname);
int rmdir(const char *pathname){
    static RMDIR old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (RMDIR)dlsym(RTLD_NEXT, "rmdir");
    return old_func(pathname);
}

#include <sys/vfs.h>    //or <sys/statfs.h>
typedef int (*STATFS)(const char *path, struct statfs *buf);
typedef int (*FSTATFS)(int fd, struct statfs *buf);
int statfs(const char *path, struct statfs *buf){
    static STATFS old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, path);
    old_func = (STATFS)dlsym(RTLD_NEXT, "statfs");
    return old_func(path, buf);
}
int fstatfs(int fd, struct statfs *buf){
    static FSTATFS old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (FSTATFS)dlsym(RTLD_NEXT, "fstatfs");
    return old_func(fd, buf);
}

typedef int (*SYMLINK)(const char *target, const char *linkpath);
int symlink(const char *target, const char *linkpath){
    static SYMLINK old_func = NULL;
    LOG_FUNC("{ target: %s, linkpath: %s }", __func__, target, linkpath);
    old_func = (SYMLINK)dlsym(RTLD_NEXT, "symlink");
    return old_func(target, linkpath);
}

typedef void (*SYNC)(void);
typedef int (*SYNCFS)(int fd);
void sync(void){
    static SYNC old_func = NULL;
    LOG_FUNC("", __func__);
    old_func = (SYNC)dlsym(RTLD_NEXT, "sync");
    return old_func();
}
int syncfs(int fd){
    static SYNCFS old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (SYNCFS)dlsym(RTLD_NEXT, "syncfs");
    return old_func(fd);
}

typedef mode_t (*UMASK)(mode_t mask);
mode_t umask(mode_t mask){
    static UMASK old_func = NULL;
    LOG_FUNC("{ mask: %o }", __func__, mask);
    old_func = (UMASK)dlsym(RTLD_NEXT, "umask");
    return old_func(mask);
}

typedef int (*UNLINK)(const char *pathname);
int unlink(const char *pathname){
    static UNLINK old_func = NULL;
    LOG_FUNC("{ path: %s }", __func__, pathname);
    old_func = (UNLINK)dlsym(RTLD_NEXT, "unlink");
    return old_func(pathname);
}

#include <utime.h>
typedef int (*UTIME)(const char *filename, const struct utimbuf *times);
int utime(const char *filename, const struct utimbuf *times){
    static UTIME old_func = NULL;
    LOG_FUNC("{ filename: %s }", __func__, filename);
    old_func = (UTIME)dlsym(RTLD_NEXT, "utime");
    return old_func(filename, times);
}

typedef ssize_t (*WRITE)(int fd, const void *buf, size_t count);
ssize_t write(int fd, const void *buf, size_t count){
    static WRITE old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (WRITE)dlsym(RTLD_NEXT, "write");
    return old_func(fd, buf, count);
}

#include <sys/uio.h>
typedef ssize_t (*READV)(int fd, const struct iovec *iov, int iovcnt);
typedef ssize_t (*WRITEV)(int fd, const struct iovec *iov, int iovcnt);
ssize_t readv(int fd, const struct iovec *iov, int iovcnt){
    static READV old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (READV)dlsym(RTLD_NEXT, "readv");
    return old_func(fd, iov, iovcnt);
}
ssize_t writev(int fd, const struct iovec *iov, int iovcnt){
    static WRITEV old_func = NULL;
    LOG_FUNC("{ fd: %d }", __func__, fd);
    old_func = (WRITEV)dlsym(RTLD_NEXT, "writev");
    return old_func(fd, iov, iovcnt);
}
