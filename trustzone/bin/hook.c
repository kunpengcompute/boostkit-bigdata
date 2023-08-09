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
//typedef int (*MUNMAP)(void *addr, size_t length);
void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
{
    static MMAP old_func = NULL;
    //LOG_FUNC("{ addr: %p length: %lu }", __func__, addr, length);
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
