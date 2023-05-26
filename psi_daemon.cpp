/*
 * Copyright (c) 2023 Qualcomm Innovation Center, Inc. All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause-Clear
 *
 * Dynamic VM Memory Resizing Daemon (psi_daemon)
 */

#define LOG_TAG "psi_daemon"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <base/logging.h>
#include <atomic>

#ifndef __weak
#define __weak __attribute__((weak))
#endif

#ifndef __unused
#define __unused __attribute__((__unused__))
#endif

#define SIZE_1MB    0x00100000
#define SIZE_1KB    0x00000400

#ifndef MS_PER_SEC
#define MS_PER_SEC  (1000)
#endif

#define US_PER_SEC  1000000
#define US_PER_MS   (US_PER_SEC / MS_PER_SEC)
#define NS_PER_SEC  1000000000
#define NS_PER_MS   (NS_PER_SEC / MS_PER_SEC)
#define NS_PER_US   (NS_PER_SEC / US_PER_SEC)

enum pressure_levels {
    PRESSURE_INVALID = -2,
    PRESSURE_NONE,
    PRESSURE_MIN = 0,

    PRESSURE_EVT_5 = PRESSURE_MIN,
    PRESSURE_EVT_10,
    PRESSURE_EVT_15,
    PRESSURE_EVT_20,
    PRESSURE_EVT_25,
    PRESSURE_EVT_30,
    PRESSURE_EVT_35,
    PRESSURE_EVT_40,
    PRESSURE_EVT_45,
    PRESSURE_EVT_50,

    PRESSURE_EVT_COUNT
};

enum zone_name {
    ZONE_NORMAL = 0,
    ZONE_MOVABLE,
    ZONE_MAX
};

static size_t sys_page_size;

static const char* const zone_names[ZONE_MAX] = {
    "Normal",
    "Movable",
};

#define MEMINFO_PATH    "/proc/meminfo"
#define ZONEINFO_PATH   "/proc/zoneinfo"
#define PSI_MEMORY_PATH "/proc/pressure/memory"

/* memory plugin size defaults (in MBs)*/
#define DEFAULT_PLUGIN_RESOLUTION_MB    (4)
#define DEFAULT_MAX_MEMORY_PLUGIN_MB    (256)

enum psi_stall_type {
    PSI_SOME,
    PSI_FULL,
    PSI_TYPE_COUNT
};

static const char* stall_type_name[] = {
        "some",
        "full",
};

struct psi_threshold {
    enum psi_stall_type stall_type;
    int threshold_ms;
};

struct psi_pressure {
    float avg10;
    float avg60;
    float avg300;
    uint64_t total;
};

struct psi_memory_pressure {
    struct psi_pressure some;
    struct psi_pressure full;
};

/* PSI threshold levels in milliseconds */
static struct psi_threshold psi_thresholds[PRESSURE_EVT_COUNT] = {
    { PSI_SOME, 5 },   { PSI_SOME, 10 },  { PSI_SOME, 15 },  { PSI_SOME, 20 },
    { PSI_SOME, 25 },  { PSI_SOME, 30 },  { PSI_SOME, 35 },  { PSI_SOME, 40 },
    { PSI_SOME, 45 },  { PSI_SOME, 50 }
};

/* global buffer for file reads */
static char* readfile_buf;
static ssize_t readfile_buf_size;

/* serialize fileread buffer accesses using mutex */
static pthread_mutex_t fileread_buffer_mutex = PTHREAD_MUTEX_INITIALIZER;

/* in MBs */
uint64_t resolution, max_plugged_memory;

/* num of memory chunks plugged-in, each of resolution MB size */
std::atomic<uint64_t> mem_chunks_plugged{0};

/* total plugged memory in VM system (in MBs) */
std::atomic<uint64_t> plugged_memory{0};

/* fds of all registered PSI events */
int32_t event_fds[PRESSURE_EVT_COUNT];

/* fd for main epoll_wait */
int32_t psi_epollfd = -1;

#define EXP_1S_10S      0.9048F     /* 1/exp(1s/10s) */
#define EXP_1S_60S      0.9834F     /* 1/exp(1s/60s) */
#define EXP_1S_300S     0.9966F     /* 1/exp(1s/300s) */

static pthread_mutex_t thread_execution_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  thread_execution_cond;
static pthread_condattr_t thread_execution_cond_attr;

std::atomic<bool> cancel_check(false);
std::atomic<bool> wait_in_progress(false);

/* default wait time for pressure to be idle (in seconds) */
#define IDLE_WAIT_TIME_S    10

/* stall tracking window size, 50ms*/
static int PSI_WINDOW_SIZE_US = (50 * US_PER_MS);

#define TARGET_OOM_SCORE_ADJ    -1000

#define LINE_MAX 250
#define STRINGIFY(x) STRINGIFY_INTERNAL(x)
#define STRINGIFY_INTERNAL(x) #x

static char str_buf[LINE_MAX];

int write_file(const char *file_path, char *s) {
    int fd;
    ssize_t len;

    fd = TEMP_FAILURE_RETRY(open(file_path, O_WRONLY | O_CLOEXEC));
    if (fd < 0) {
        LOG(ERROR) << file_path << " open failed, err: " << strerror(errno);
        return -EINVAL;
    }

    len = write(fd, s, strlen(s));
    if (len < (ssize_t)strlen(s)) {
        LOG(ERROR) << "error writing to file: " << file_path << "val " << s;
        close(fd);
        return -EINVAL;
    }

    close(fd);
    return 0;
}

char *read_file(const char *file_path) {
    int fd;
    ssize_t readsize;
    char *new_buf = NULL;

    fd = TEMP_FAILURE_RETRY(open(file_path, O_RDONLY | O_CLOEXEC));
    if (fd < 0) {
        LOG(ERROR) << file_path << " open failed, err: " << strerror(errno);
        return NULL;
    }

    while ((readsize = TEMP_FAILURE_RETRY(pread(fd, readfile_buf,
                readfile_buf_size, 0))) == readfile_buf_size) {
        /*
         * if file content is more than readfile buffer size, resize the buffer
         * to double its previous size with realloc
         */
        readfile_buf_size *= 2;
        new_buf = (char *)realloc(readfile_buf, readfile_buf_size);
        if (new_buf == NULL) {
            LOG(ERROR) << "resizing fileread buffer failed, errno: " <<
                    strerror(errno);
            close(fd);
            return NULL;
        }
        readfile_buf = new_buf;
    }

    readfile_buf[readsize] = 0;
    close(fd);
    return readfile_buf;
}

/*
 * no strtok_r since that modifies buffer and we want to use multiline sscanf
 */
static char *nextln(char *buf)
{
    char *x;

    x = (char *)memchr(buf, '\n', strlen(buf));
    if (!x)
        return buf + strlen(buf);
    return x + 1;
}

static int parse_field(char *buf, const char *field_name, uint64_t *val) {
    char *save_ptr;
    char *line;
    int nargs;

    if (field_name == NULL) {
        LOG(ERROR) << __func__ << ": field name for parsing is null";
        goto err;
    }

    for (line = strtok_r(buf, "\n", &save_ptr); line;
        line = strtok_r(NULL, "\n", &save_ptr)) {
        if(strstr(line, field_name) == NULL)
          continue;
        /* found our line */
        nargs = sscanf(line, "%*[^0-9]%lu", val);
        if (nargs != 1) {
            LOG(ERROR) << "parsing field value " << field_name << " in line " <<
                    line << " failed";
            goto err;
        }
        return 0;
    }

    LOG(ERROR) << __func__ <<": field name " << field_name << " not found";
err:
    return -EINVAL;
}

static int parse_system_zoneinfo(const char *zone_name, const char *field_name, uint64_t *val) {
    char name[LINE_MAX + 1];    /* LINE_MAX + 1 to avoid sscanf overflow */
    char *buf;
    int nargs, field_val = -1;

    pthread_mutex_lock(&fileread_buffer_mutex);
    buf = read_file(ZONEINFO_PATH);
    if (!buf)
        goto err;

    while (*buf) {
        nargs = sscanf(buf, "Node %*u, zone %" STRINGIFY(LINE_MAX) "s", name);
        buf = nextln(buf);
        if (nargs == 1 && !strcmp(name, zone_name))
            break;
    }

    if (!*buf)
        goto err;

    field_val = parse_field(buf, field_name, val);

err:
    pthread_mutex_unlock(&fileread_buffer_mutex);
    return field_val;
}

static int parse_system_meminfo(const char *field_name, uint64_t *val) {
    char *buf;
    int field_val = -1;

    pthread_mutex_lock(&fileread_buffer_mutex);
    buf = read_file(MEMINFO_PATH);
    if (!buf)
        goto err;

    field_val = parse_field(buf, field_name, val);

err:
    pthread_mutex_unlock(&fileread_buffer_mutex);
    return field_val;
}

static uint64_t get_zone_memfree_kb(const char *name) {
    uint64_t zone_memfree = 0;
    int res;

    res = parse_system_zoneinfo(name, "pages free", &zone_memfree);
    if (res) {
        LOG(ERROR) << "parsing system zoneinfo failed, errno: " <<
                strerror(errno);
        return 0;
    }

    return (((uint64_t)zone_memfree * sys_page_size) / SIZE_1KB);
}

static uint64_t get_system_memfree_kb() {
    uint64_t system_memfree = 0;
    int res;

    res = parse_system_meminfo("MemFree", &system_memfree);
    if (res) {
        LOG(ERROR) << "parsing system meminfo failed, errno: " <<
                strerror(errno);
        return 0;
    }

    return system_memfree;
}

static int parse_system_psi_memory(struct psi_memory_pressure *psi_memory)
{
    char *buf;
    int nargs, res = -1;

    pthread_mutex_lock(&fileread_buffer_mutex);
    buf = read_file(PSI_MEMORY_PATH);
    if (!buf)
        goto err;

    memset(psi_memory, 0, sizeof(struct psi_memory_pressure));

    /* get memory pressure for SOME */
    nargs = sscanf(buf, "some avg10=%f avg60=%f avg300=%f total=%lu",
                &psi_memory->some.avg10,
                &psi_memory->some.avg60,
                &psi_memory->some.avg300,
                &psi_memory->some.total);
    if (nargs != 4) {
        LOG(ERROR) << "line parse for SOME avgs failed";
        goto err;
    }

    buf = nextln(buf);

    /* get memory pressure for FULL */
    nargs = sscanf(buf, "full avg10=%f avg60=%f avg300=%f total=%lu",
                &psi_memory->full.avg10,
                &psi_memory->full.avg60,
                &psi_memory->full.avg300,
                &psi_memory->full.total);
    if (nargs != 4) {
        LOG(ERROR) << "line parse for FULL avgs failed";
        goto err;
    }

    res = 0;
err:
    pthread_mutex_unlock(&fileread_buffer_mutex);
    return res;
}

static int register_psi_memory(enum psi_stall_type stall_type,
             int threshold_us, int window_us) {
    int fd;
    int res;
    char buf[LINE_MAX];

    fd = TEMP_FAILURE_RETRY(open(PSI_MEMORY_PATH, O_WRONLY | O_CLOEXEC));
    if (fd < 0) {
        LOG(ERROR) << "No kernel psi monitor support, errno: " <<
                strerror(errno);
        return -1;
    }

    /* monitor memory pressure for partial stall */
    snprintf(buf, sizeof(buf), "%s %d %d",
                stall_type_name[stall_type], threshold_us, window_us);

    res = TEMP_FAILURE_RETRY(write(fd, buf, strlen(buf) + 1));
    if (res < 0) {
        LOG(ERROR) << PSI_MEMORY_PATH << " write failed for psi stall type " <<
                "'" << stall_type_name[stall_type] << "', errno: " <<
                strerror(errno);
        goto err;
    }

    return fd;

err:
    close(fd);
    return -1;
}

static int register_epoll_events(int epollfd, int psi_event_fd, void *data) {
    int res;
    struct epoll_event epevent;

    /* register for epoll with EPOLLPRI and EPOLLWAKEUP events */
    epevent.events = EPOLLPRI | EPOLLWAKEUP;
    epevent.data.ptr = data;
    res = epoll_ctl(epollfd, EPOLL_CTL_ADD, psi_event_fd, &epevent);
    if (res < 0) {
        LOG(ERROR) << "epoll_ctl for psi monitor failed, errno: " <<
                strerror(errno);
    }
    return res;
}

static int unregister_epoll_events(int epollfd, int psi_event_fd) {
    return epoll_ctl(epollfd, EPOLL_CTL_DEL, psi_event_fd, NULL);
}

static void unregister_psi_events() {
    int num;

    for (num = PRESSURE_MIN; num < PRESSURE_EVT_COUNT; num ++) {
        if (event_fds[num] >= 0) {
            unregister_epoll_events(psi_epollfd, event_fds[num]);
            close(event_fds[num]);
        }
        event_fds[num] = -1;
    }
}

static const char *psi_level_to_string(pressure_levels level) {
    uint16_t num;

    if (level == PRESSURE_NONE)
        return "NONE";
    if (level == PRESSURE_INVALID)
        return "INVALID";

    num = psi_thresholds[level].threshold_ms;
    memset(str_buf, 0, sizeof(str_buf));
    snprintf(str_buf, sizeof(str_buf), "EVENT_%dMS", num);

    return str_buf;
}

static int init_and_register_psi_events() {

    int num;

    for (num = PRESSURE_MIN; num < PRESSURE_EVT_COUNT; num ++)
        event_fds[num] = -1;

    psi_epollfd = epoll_create(PRESSURE_EVT_COUNT);
    if (psi_epollfd == -1) {
        LOG(ERROR) << "epoll_create failed, errno: " << strerror(errno);
        return -1;
    }

    for (num = PRESSURE_MIN; num < PRESSURE_EVT_COUNT; num ++) {

        /* register to psi moniters */
        event_fds[num] = register_psi_memory(psi_thresholds[num].stall_type,
                psi_thresholds[num].threshold_ms * US_PER_MS,
                PSI_WINDOW_SIZE_US);

        if (event_fds[num] < 0) {
            LOG(ERROR) << "PSI init failed for event " <<
                    psi_level_to_string((enum pressure_levels)num) <<
                    ", fd: " << event_fds[num];
            goto fail;
        }

        /* initialize epoll events */
        if (register_epoll_events(psi_epollfd, event_fds[num],
                                 (void*)((uint64_t) num)) != 0) {
            LOG(ERROR) << "PSI registration failed for event " <<
                    psi_level_to_string((enum pressure_levels)num);
            goto fail;
        }
    }

    return 0;

fail:
    unregister_psi_events();
    close(psi_epollfd);
    psi_epollfd = -1;
    return -1;

}

static int32_t calc_time_to_decay(struct psi_memory_pressure psi_memory)
{
    int32_t avg10_decay = 0, avg60_decay = 0;
    float avg;

    /* calc exponential time decay time for 10 seconds average */
    avg = psi_memory.some.avg10 * EXP_1S_10S;
    /*
     * we need avg10 to be 0.00 and the least is 0.01 value. Calculate avg10
     * time to decay until value is less than 0.009F to avoid floating
     * point approximations.
     */
    while (avg > 0.009F) {
        ++avg10_decay;
        avg *= EXP_1S_10S;
    }

    /* calc exponential time decay time for 60 seconds average */
    avg = psi_memory.some.avg60 * EXP_1S_60S;
    /* acceptable pressure during 60 sec window is 18 ms (0.03% of 60 sec) */
    while (avg > 0.03F) {
        ++avg60_decay;
        avg *= EXP_1S_60S;
    }

    return (std::max(avg10_decay, avg60_decay));
}

static bool inline get_atomic_variable_bool(std::atomic<bool> &variable) {
    return variable.load();
}

static void inline set_atomic_variable_bool(std::atomic<bool> &variable, bool val) {
    variable.exchange(val);
}

static void wait_for_condition_timed(pthread_cond_t *condition,
        pthread_mutex_t *mutex, const struct timespec *timeout_ts) {

    if (!condition || !mutex)
        return;

    pthread_mutex_lock(mutex);
    if (timeout_ts)
        pthread_cond_timedwait(condition, mutex, timeout_ts);
    else
        /* timeout_ts = NULL means wait indefinitely with no timeout */
        pthread_cond_wait(condition, mutex);
    pthread_mutex_unlock(mutex);
}

static void wait_until_pressure_decay(void)
{
    struct psi_memory_pressure pressure;
    struct timespec timeout;
    int32_t decay_time;
    int res;

    LOG(DEBUG) << "waiting for pressure to decay...";

    while(1) {
        res = parse_system_psi_memory(&pressure);
        if (res < 0) {
            LOG(ERROR) << "parsing system psi memory failed, errno: " <<
                    strerror(errno);
            break;
        }

        if (pressure.some.avg10 == 0.00 && pressure.some.avg60 <= 0.03) {
            LOG(DEBUG) << "PSI memory avgs are below the set thresholds";
            break;
        }

        decay_time = calc_time_to_decay(pressure);

        /* add another 2 seconds as extra cushion time for decay to settle */
        decay_time += 2;

        /* sleep until decay.. and break if CANCEL signal is sent */
        clock_gettime(CLOCK_MONOTONIC, &timeout);
        timeout.tv_sec += decay_time;

        LOG(INFO) << "sleeping for " << decay_time << " seconds for pressure to decay";
        wait_for_condition_timed(&thread_execution_cond,
                &thread_execution_mutex, &timeout);

        /* check if we recived new pressure event. If so, we may want to break
         * from this pressure decay wait loop and return to pthread main loop.
         */
        if (get_atomic_variable_bool(cancel_check))
            return;
    }
        LOG(DEBUG) << "Out of pressure to decay";
}

/*
 * downstream implementation of memory plugin and unplug request
 * are needed to support the functionality of psi_daemon.
 */

int __weak memory_plug_init(void) {
    LOG(ERROR) << "Memory plug request not supported";
    return -ENOTTY;
}

void __weak memory_plug_deinit(void) {
    LOG(ERROR) << "Memory plug request not supported";
}

int __weak memory_plug_request(uint64_t __unused size) {
    LOG(ERROR) << "Memory plug request not supported";
    return -ENOTTY;
}

int __weak memory_unplug_request(uint64_t __unused size) {
    LOG(ERROR) << "Memory unplug request not supported";
    return -ENOTTY;
}

int __weak memory_unplug_request_kernel(size_t __unused count) {
    LOG(ERROR) << "Memory unplug request kernel not supported";
    return -ENOTTY;
}

int __weak memory_unplug_all_request(void) {
    LOG(ERROR) << "Memory unplug all request not supported";
    return -ENOTTY;
}

int __weak get_memory_plugin_resolution(uint64_t *plugin_resolution_mb) {
    *plugin_resolution_mb = DEFAULT_PLUGIN_RESOLUTION_MB;
    return 0;
}

int __weak get_max_memory_plugin_allowed(uint64_t *max_memory_plugin_mb) {
    *max_memory_plugin_mb = DEFAULT_MAX_MEMORY_PLUGIN_MB;
    return 0;
}

int __weak get_kernel_plugin_count(size_t __unused *count) {
    return -ENOTTY;
}

void* memtrack_thread_function(void *arg) {

    (void)(arg);
    struct timespec timeout;
    uint64_t movable_free_kb = 0, normal_free_kb = 0, sys_memfree_kb = 0;
    uint64_t count = 0, mem_chunks_unplugged = 0;
    uint64_t kernel_count = 0, kernel_chunks_plugged = 0;
    int res;

    while (1) {

        /* lets wait for notify */
        wait_for_condition_timed(&thread_execution_cond,
                &thread_execution_mutex, NULL);

startover:
        mem_chunks_unplugged = 0;
        clock_gettime(CLOCK_MONOTONIC, &timeout);
        timeout.tv_sec += IDLE_WAIT_TIME_S;

        set_atomic_variable_bool(wait_in_progress, true);

        /* wait for IDLE_WAIT_TIME_S seconds of idle in memory pressure */
        wait_for_condition_timed(&thread_execution_cond,
                &thread_execution_mutex, &timeout);

        /* check if we received any new memory pressure event during idle time wait */
        if (get_atomic_variable_bool(cancel_check)) {
            set_atomic_variable_bool(cancel_check, false);
            set_atomic_variable_bool(wait_in_progress, false);
            goto startover;
        }

        /*
         * we have passed the IDLE_WAIT_TIME_S seconds of idle in memory pressure.
         * its safe now to assume that no memory consuming usescases are running.
         */

        /* wait until pressure is decayed to avg10=0.0 and avg60<0.5 */
        wait_until_pressure_decay();

        /* check if we received any new memory pressure event during pressure decay wait */
        if (get_atomic_variable_bool(cancel_check)) {
            set_atomic_variable_bool(cancel_check, false);
            set_atomic_variable_bool(wait_in_progress, false);
            goto startover;
        }

        /* didn't receive any new memory pressure events, so time to release memory to PVM */
        //TODO: add more checks for memory stats such as reclaimable memory, zram etc.
        movable_free_kb = get_zone_memfree_kb(zone_names[ZONE_MOVABLE]);
        normal_free_kb = get_zone_memfree_kb(zone_names[ZONE_NORMAL]);
        sys_memfree_kb = get_system_memfree_kb();

        LOG(DEBUG) << "MemFree before UNPLUG: " << sys_memfree_kb <<
                " KB (Normal: " << normal_free_kb << " KB, Movable: " <<
                movable_free_kb << " KB)";

        count = (movable_free_kb / SIZE_1KB) / resolution;
        LOG(DEBUG) << "Count " << count << " mem_chunks_plugged " <<
                mem_chunks_plugged.load() << " resolution " << resolution <<
                " MB plugged_memory " << plugged_memory.load() << " MB";

        /* first release blocks added by kernel */
        if (get_kernel_plugin_count(&kernel_chunks_plugged) < 0) {
            LOG(ERROR) << "failed to get kernel plugin count";
            goto release_blocks;
        }
        LOG(INFO) << "kernel_chunks_plugged: " << kernel_chunks_plugged;

        kernel_count = std::min(count, kernel_chunks_plugged);
        memory_unplug_request_kernel(kernel_count);

        /* get kernel plugin count after write */
        if (get_kernel_plugin_count(&kernel_count) < 0) {
            LOG(ERROR) << "failed to get kernel plugin count";
            goto release_blocks;
        }
        LOG(INFO) << "kernel_count after write: " << kernel_count;

        if (kernel_count <= kernel_chunks_plugged)
            count -= (kernel_chunks_plugged - kernel_count);

release_blocks:
        count = std::min(count, mem_chunks_plugged.load());
        LOG(INFO) << "Count after kernel unplug: " << count;

        while(count-- > 0) {
            res = memory_unplug_request(resolution);
            if (res) {
                LOG(ERROR) << "Failed to unplug one memory chunk of " <<
                        resolution << "MB";
                continue;
            }
            mem_chunks_unplugged++;
        }

        if (mem_chunks_unplugged)
            LOG(INFO) << "Unplugged " << mem_chunks_unplugged <<
                " memory chunks. Total memory unplugged: " <<
                mem_chunks_unplugged << " MB";
        plugged_memory -= (resolution * mem_chunks_unplugged);
        mem_chunks_plugged -= mem_chunks_unplugged;

        normal_free_kb = get_zone_memfree_kb(zone_names[ZONE_NORMAL]);
        movable_free_kb = get_zone_memfree_kb(zone_names[ZONE_MOVABLE]);
        sys_memfree_kb = get_system_memfree_kb();

        LOG(DEBUG) << "MemFree after UNPLUG: " << sys_memfree_kb <<
                " KB (Normal: " << normal_free_kb << " KB, Movable: " <<
                movable_free_kb<< " KB)";
        //TODO: should we keep checking until all plugged memory is unplugged? goto startover ?

        /* now lets wait for notify again... */

    }

    LOG(ERROR) << "pthread exiting..";
    return (void *)NULL;
}

static pressure_levels psi_wait_for_pressure(void) {

    pressure_levels pressure_level = PRESSURE_NONE;
    struct epoll_event events[PRESSURE_EVT_COUNT];
    int nevents = 0;

    do {
        if (pressure_level == PRESSURE_NONE) {
             /* Wait for events with no timeout */
            nevents = epoll_wait(psi_epollfd, events, PRESSURE_EVT_COUNT, -1);
        } else {
            /* Assume that the memory pressure state will stay high for at least 1s.
             * Within that 1s window, the memory pressure state can go up due to
             * a different FD becoming available or it can go down when that window expires.
             * Accordingly, there's no polling: just epoll_wait with a 1s timeout.
             */
            nevents = epoll_wait(psi_epollfd, events, PRESSURE_EVT_COUNT, 1000);
            if (nevents == 0) {
                pressure_level = PRESSURE_NONE;
                return pressure_level;
            }
        }
        /* keep waiting if interrupted */
    } while (nevents == -1 && errno == EINTR);

    if (nevents == -1) {
        LOG(ERROR) << "epoll_wait failed while waiting for psi events: " <<
                strerror(errno);
        return PRESSURE_INVALID;
    }
    /* reset pressure_level and raise it based on received events */
    pressure_level = PRESSURE_NONE;
    for (int i = 0; i < nevents; i++) {
        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
            /* should never happen unless psi got disabled in kernel */
            LOG(ERROR) << "Memory pressure events are not available anymore";
            return PRESSURE_INVALID;
        }

        /* record the highest reported level */
        if ((pressure_levels)events[i].data.u32 > pressure_level) {
            pressure_level = (pressure_levels) events[i].data.u32;
        }
    }

    return pressure_level;
}

static uint64_t get_timespec_delta_us(struct timespec start_tv, struct timespec end_tv) {
    uint64_t start_us;
    uint64_t end_us;

    end_us = (end_tv.tv_nsec / NS_PER_US) + (end_tv.tv_sec * US_PER_SEC);
    start_us = (start_tv.tv_nsec / NS_PER_US) + (start_tv.tv_sec * US_PER_SEC);

    return (end_us - start_us);
}

static void psi_mainloop(void) {
    pressure_levels pressure_level = PRESSURE_NONE;
    uint64_t normal_free_kb = 0, movable_free_kb = 0, sys_memfree_kb = 0;
    struct timespec cur, start, end;
    int res;

    while (1) {
        pressure_level = psi_wait_for_pressure();
        clock_gettime(CLOCK_MONOTONIC, &cur);

        if (pressure_level == PRESSURE_INVALID) {
            LOG(DEBUG) << "Error waiting for PSI event";
            break;
        }

        if (pressure_level == PRESSURE_NONE) {
            LOG(DEBUG) << "No PSI events received. epoll_wait again...";
            continue;
        }

        LOG(DEBUG) << "Received pressure event "<<
                psi_level_to_string(pressure_level) <<
                ". TIME: " << cur.tv_sec << "." << cur.tv_nsec/NS_PER_MS <<" s";

        normal_free_kb = get_zone_memfree_kb(zone_names[ZONE_NORMAL]);
        movable_free_kb = get_zone_memfree_kb(zone_names[ZONE_MOVABLE]);
        sys_memfree_kb = get_system_memfree_kb();
        LOG(DEBUG) << "MemFree: " << sys_memfree_kb <<
                " KB (Normal: " << normal_free_kb << " KB, Movable: " <<
                movable_free_kb << " KB)";

        /*
         * if memtrack pthread is waiting on pressure decay, notify to
         * start over since we received new pressure event.
         */
        if (get_atomic_variable_bool(wait_in_progress))
            set_atomic_variable_bool(cancel_check, true);

        /* notify memtrack thread that we received new memory pressure event */
        pthread_cond_signal(&thread_execution_cond);

        //TODO: add more checks for memory stats such as reclaimable memory, zram etc.
        if ((pressure_level >= PRESSURE_EVT_5) &&        /* min threshold reached */
            (plugged_memory.load() < (uint64_t)max_plugged_memory) &&    /* max memory boundary */
            (movable_free_kb < ((uint64_t)resolution * SIZE_1KB) / 2)) {    /* only if movable < 1/2 * resolution */

            LOG(DEBUG) << "Plugging-in " << resolution << "MB of memory";

            clock_gettime(CLOCK_MONOTONIC, &start);
            /*
             * TODO: push memory_plug_request call onto separate thread
             * so that the main thread can continue with PSI monitoring
             */
            res = memory_plug_request(resolution);
            clock_gettime(CLOCK_MONOTONIC, &end);

            if (res < 0) {
                LOG(ERROR) << "memory plugin request for " <<
                        resolution << "MB FAILED";
                break;
            }

            plugged_memory += resolution;
            mem_chunks_plugged++;

            LOG(DEBUG) << "Plugged-in " << resolution <<" MB of memory: SUCCESS";
            LOG(INFO) << "Time taken to plug-in " << resolution <<" MB: "<<
                    get_timespec_delta_us(start, end) <<" us. TIME: "<<
                    end.tv_sec << "." << end.tv_nsec/NS_PER_MS <<" s";

            LOG(INFO) << "Total memory plugged-in so far: " <<
                    plugged_memory.load() <<
                    " MB. Total memory chunks plugged-in: "<<
                    mem_chunks_plugged.load();
            normal_free_kb = get_zone_memfree_kb(zone_names[ZONE_NORMAL]);
            movable_free_kb = get_zone_memfree_kb(zone_names[ZONE_MOVABLE]);
            sys_memfree_kb = get_system_memfree_kb();

            LOG(DEBUG) << "MemFree after PLUG: " << sys_memfree_kb <<
                    " KB (Normal: " << normal_free_kb << " KB, Movable: " <<
                    movable_free_kb<< " KB)";
        }
    }
}

void set_oom_score_adj_self(int adj)
{
    char path[LINE_MAX] = "/proc/self/oom_score_adj";
    char val[LINE_MAX];
    int fd;

    snprintf(val, sizeof(val), "%d", adj);

    fd = open(path, O_WRONLY);
    if (fd < 0) {
        LOG(ERROR) <<"Couldn't open " << path;
        return;
    }

    if (write(fd, val, strlen(val)) < 0)
        LOG(ERROR) << "Couldn't write to "<< path;

    close(fd);
}

int main(void) {

    int i;
    pthread_t memtrack_thread;
    std::string thresholds;
    char str[LINE_MAX];

    /* get system PAGE_SIZE */
    sys_page_size = sysconf(_SC_PAGE_SIZE);
    if (!sys_page_size) {
        LOG(ERROR) << "Getting system page size failed";
        return -EINVAL;
    }

    readfile_buf_size = sys_page_size;

    /* allocate buffer for file reads */
    readfile_buf = (char *)calloc(readfile_buf_size, sizeof(*readfile_buf));
    if (!readfile_buf) {
        LOG(ERROR) << "Buffer allocation for file reads failed";
        return -ENOMEM;
    }

    if (memory_plug_init()) {
        LOG(ERROR) << "Memory plugin init failed";
        return -EINVAL;
    }


    /* Initialize PSI monitors */
    if (init_and_register_psi_events()) {
        LOG(ERROR) << "Registering to PSI events failed";
        return -EINVAL;
    }

    pthread_condattr_init(&thread_execution_cond_attr);

    /* set clock type to CLOCK_MONOTONIC */
    pthread_condattr_setclock(&thread_execution_cond_attr, CLOCK_MONOTONIC);

    /* initialize condition variable using the attribute */
    pthread_cond_init(&thread_execution_cond, &thread_execution_cond_attr);

    /* create pthread for downword memory pressure tracking */
    if (pthread_create(&memtrack_thread, NULL, &memtrack_thread_function, NULL)) {
        LOG(ERROR) << "Error creating pthread for downward mem tracking";
        return -EINVAL;
    }

    pthread_condattr_destroy(&thread_execution_cond_attr);


    for (i = 0; i < PRESSURE_EVT_COUNT; i++) {
        snprintf(str, sizeof(str), "%dMS ", psi_thresholds[i].threshold_ms);
        thresholds.append(str);
    }
    LOG(INFO) << "PSI init completed! Thresholds: " << thresholds;

    set_oom_score_adj_self(TARGET_OOM_SCORE_ADJ);

     get_memory_plugin_resolution(&resolution);
     get_max_memory_plugin_allowed(&max_plugged_memory);
    LOG(INFO) << "Memory plug-in resolution: " << resolution <<" MB";
    LOG(INFO) << "Maximum memory plug-in allowed: " << max_plugged_memory <<" MB";

    LOG(INFO) << "Waiting for pressure events...";
    psi_mainloop();

    /* should not exit */
    LOG(ERROR) << "Exiting...";

    memory_plug_deinit();
    return 0;
}
