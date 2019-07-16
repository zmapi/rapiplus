import fcntl
import sys
import struct

# fcntl(2) - Linux man page:
# struct flock {
#     ...
#     short l_type;    /* Type of lock: F_RDLCK,
#                         F_WRLCK, F_UNLCK */
#     short l_whence;  /* How to interpret l_start:
#                         SEEK_SET = 0, SEEK_CUR = 1, SEEK_END = 2 */
#     off_t l_start;   /* Starting offset for lock */
#     off_t l_len;     /* Number of bytes to lock */
#     pid_t l_pid;     /* PID of process blocking our lock
#                         (F_GETLK only) */
#     ...
# };

FLOCK_HEADER = "hhllh"

W_SEEK_SET = 0 # beginning of file
W_SEEK_CUR = 1 # current location of file
W_SEEK_END = 2 # end of file

def fcntl_lock(f, cmd, lock_type, whence, start_offset, num_bytes):
    flock = struct.pack(FLOCK_HEADER,
                        lock_type,
                        whence,
                        start_offset,
                        num_bytes,
                        0)
    res = fcntl.fcntl(f, cmd, flock)
    res = struct.unpack(FLOCK_HEADER, res)
    return res

def open_with_lock(fn, mode="r", **kwargs):
    blocking = kwargs.pop("blocking", True)
    if blocking:
        cmd = fcntl.F_SETLKW
    else:
        cmd = fcntl.F_SETLK
    f = open(fn, mode, **kwargs)
    if "w" in mode or "a" in mode:
        lock_type = fcntl.F_WRLCK
    else:
        lock_type = fcntl.F_RDLCK
    # lock whole file
    fcntl_lock(f, cmd, lock_type, W_SEEK_SET, 0, 0)
    return f

# def lock(fn, lock_type, blocking=True):
#     if blocking:
#         cmd = fcntl.F_SETLKW
#     else:
#         cmd = fcntl.F_SETLK
#     f = open(fn, "r")
#     # lock whole file
#     fcntl_lock(f, cmd, lock_type, W_SEEK_SET, 0, 0)
#     return f
# 
# def write_lock(fn, blocking=True):
#     lock(fn, fcntl.F_WRLCK, blocking)
# 
# def read_lock(fn, blocking=True):
#     lock(fn, fcntl.F_RDLCK, blocking)
