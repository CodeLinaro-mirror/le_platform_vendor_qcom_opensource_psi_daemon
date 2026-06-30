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
#include <sstream>
#include <time.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <base/logging.h>
#include <atomic>
#include <signal.h>

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
#define VMSTAT_PATH     "/proc/vmstat"
#define PSI_MEMORY_PATH "/proc/pressure/memory"

#define WAKE_LOCK_PATH   "/sys/power/wake_lock"
#define WAKE_UNLOCK_PATH "/sys/power/wake_unlock"
#define WAKELOCK_STR     "psi_daemon_lock"
#define WAKEUNLOCK_STR   WAKELOCK_STR

/* memory plugin size defaults (in MBs)*/
#define DEFAULT_PLUGIN_RESOLUTION_MB    (4)
#define DEFAULT_MAX_MEMORY_PLUGIN_MB    (256)

#define MAX_UNPLUG_RETRY                (10)

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

struct memory_snapshot {
    uint64_t sys_memfree_kb;
    uint64_t normal_free_kb;
    uint64_t movable_free_kb;
    uint64_t movable_inactive_anon_kb;
    uint64_t movable_inactive_file_kb;
    uint64_t pgalloc_normal_nr;
    uint64_t pgalloc_movable_nr;
    uint64_t swap_free_kb;
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

/*
 * we wait until memory pressure decays below certain
 * threshold before unplugging memory.
 */
static bool enable_pressure_decay_wait = false;

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

struct timespec last_remove_time;

/*
 * Hold-off window after a memblock remove.  Scaled dynamically by
 * get_holdoff_ms_after_remove() based on how many blocks were removed;
 * this variable tracks the value used for the most recent removal.
 */
static uint32_t last_remove_holdoff_ms = 2000;

#define EXP_1S_10S      0.9048F     /* 1/exp(1s/10s) */
#define EXP_1S_60S      0.9834F     /* 1/exp(1s/60s) */
#define EXP_1S_300S     0.9966F     /* 1/exp(1s/300s) */

static pthread_mutex_t thread_execution_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  thread_execution_cond;
static pthread_condattr_t thread_execution_cond_attr;

std::atomic<bool> cancel_check(false);
std::atomic<bool> wait_in_progress(false);

/* minimal increase in nr_pgalloc pages, which can be considered as allocation activity */
#define PGALLOC_GROWTH_THRESHOLD      1000

/* nr_pfalloc of Movable zone after last unplug of a block */
uint64_t movable_pgalloc_last_unplug;

/* default wait time for pressure to be idle (in seconds) */
/*
 * Base idle wait before attempting unplug.  The actual wait is computed
 * adaptively by get_idle_wait_time_s() based on how many blocks are
 * currently plugged: a heavier usecase plugs more blocks and warrants a
 * longer observation window before we conclude it has ended.
 */
#define IDLE_WAIT_TIME_S_BASE  3
#define IDLE_WAIT_TIME_S_MED   8
#define IDLE_WAIT_TIME_S_HEAVY 15

/* Maximum blocks to unplug in a single cycle to avoid large sudden drops */
#define MAX_BLOCKS_PER_UNPLUG_CYCLE  5

/* Settling time between consecutive single-block removals (seconds) */
#define UNPLUG_SETTLE_TIME_S    1

/* The retry timer is for retrying unplug procedure if there are still
 * blocks present after the current unplug procedure.
 * */
#define RETRY_WAIT_TIME_S   1

/*
 * minimum Movable zone managed memory to be persistent always (in MBs).
 * This value is obtained from target specific compile flag.
 * Reset it to 0 if flag isn't defined.
 */
#ifndef MIN_MOVABLE_SIZE_PERSISTENT_MB
#define MIN_MOVABLE_SIZE_PERSISTENT_MB   0
#endif

/* stall tracking window size, 50ms*/
static int PSI_WINDOW_SIZE_US = (50 * US_PER_MS);
/*
 * The percent reduction in size of anonymous memory after being
 * swapped out and compressed by zram.
 */
static const int ZRAM_COMPRESSION_RATIO_PCT = 50;

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

/* returns 0 on failure */
static unsigned int wake_lock_acquire()
{
    char str_val[LINE_MAX];

    snprintf(str_val, sizeof(str_val), WAKELOCK_STR);
    if (write_file(WAKE_LOCK_PATH, str_val)) {
        LOG(ERROR) << "Failed to write to " << WAKE_LOCK_PATH <<
                " errno: " << strerror(errno);
        return 0;
    }

    return 1;
}

static unsigned int wake_unlock()
{
    char str_val[LINE_MAX];

    snprintf(str_val, sizeof(str_val), WAKEUNLOCK_STR);
    if (write_file(WAKE_UNLOCK_PATH, str_val)) {
        LOG(ERROR) << "Failed to write to " << WAKE_UNLOCK_PATH <<
                " errno: " << strerror(errno);
        return 0;
    }

    return 1;
}

static int parse_field(char *buf, const char *field_name, uint64_t *val) {
    std::istringstream sstr(buf);
    std::string line;
    int nargs;

    if (field_name == NULL) {
        LOG(ERROR) << __func__ << ": field name for parsing is null";
        goto err;
    }

    while (std::getline(sstr, line)) {
        if(strstr(line.c_str(), field_name) == NULL)
            continue;
        /* found our line */
        nargs = sscanf(line.c_str(), "%*[^0-9]%lu", val);
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

/* must be used with fileread_buffer_mutex lock taken */
static char *get_zoneinfo(void)
{
    return read_file(ZONEINFO_PATH);
}

static char *get_meminfo(void)
{
    return read_file(MEMINFO_PATH);
}

static char *get_vmstat(void)
{
    return read_file(VMSTAT_PATH);
}

static int parse_zone_field(char *buf, const char *zone_name,
        const char *field_name, uint64_t *val)
{
    char name[LINE_MAX + 1];    /* LINE_MAX + 1 to avoid sscanf overflow */
    int nargs;

    if (!buf)
        return -EINVAL;

    while (*buf) {
        nargs = sscanf(buf, "Node %*u, zone %" STRINGIFY(LINE_MAX) "s", name);
        buf = nextln(buf);
        if (nargs == 1 && !strcmp(name, zone_name))
            break;
    }

    if (!*buf)
        return -EINVAL;

    return parse_field(buf, field_name, val);
}

static int system_memory_snapshot(struct memory_snapshot *mem_snap)
{
    char *buf;
    int res = -1;

    pthread_mutex_lock(&fileread_buffer_mutex);

    /***** get zoneinfo *****/

    buf = get_zoneinfo();
    if (!buf)
        goto err;

    /*
     * some zone fields maynot be present if zone is empty.
     * so not necessarily parsing errors. return 0 for fields
     * that doesn't exists.
     */

    if (parse_zone_field(buf, zone_names[ZONE_NORMAL],
            "pages free", &mem_snap->normal_free_kb))
        mem_snap->normal_free_kb = 0;
    mem_snap->normal_free_kb *= sys_page_size / SIZE_1KB;

    if (parse_zone_field(buf, zone_names[ZONE_MOVABLE],
            "pages free", &mem_snap->movable_free_kb))
        mem_snap->movable_free_kb = 0;
    mem_snap->movable_free_kb *= sys_page_size / SIZE_1KB;

    if (parse_zone_field(buf, zone_names[ZONE_MOVABLE],
            "nr_zone_inactive_anon", &mem_snap->movable_inactive_anon_kb))
        mem_snap->movable_inactive_anon_kb = 0;
    mem_snap->movable_inactive_anon_kb *= sys_page_size / SIZE_1KB;

    if (parse_zone_field(buf, zone_names[ZONE_MOVABLE],
            "nr_zone_inactive_file", &mem_snap->movable_inactive_file_kb))
        mem_snap->movable_inactive_file_kb = 0;
    mem_snap->movable_inactive_file_kb *= sys_page_size / SIZE_1KB;

    /***** get meminfo *****/

    buf = get_meminfo();
    if (!buf)
        goto err;

    if(parse_field(buf, "MemFree", &mem_snap->sys_memfree_kb))
        goto err;
    if(parse_field(buf, "SwapFree", &mem_snap->swap_free_kb))
	    goto err;

    /***** get vmstat *****/

    buf = get_vmstat();
    if (!buf)
        goto err;

    if(parse_field(buf, "pgalloc_normal", &mem_snap->pgalloc_normal_nr))
        goto err;
    if(parse_field(buf, "pgalloc_movable", &mem_snap->pgalloc_movable_nr))
        goto err;

    res = 0;

err:
    if (res)
        LOG(ERROR) << "failed to get memory snapshot";
    pthread_mutex_unlock(&fileread_buffer_mutex);
    return res;
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
    epevent.events = EPOLLPRI;
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

int __weak get_total_block_plugin_count(size_t __unused *count) {
    return -ENOTTY;
}

static uint64_t get_memsnap_and_total_free(struct memory_snapshot *mem_snap)
{
    uint64_t free;

    if (system_memory_snapshot(mem_snap))
        return 0;

   /*
    * inactive_file pages can be reclaimed easily, and
    * inactive_anon pages can be swapped and reused.
    *
    * Discount inactive anon by a factor of 2 assuming a compression ratio
    * of 50% with zram.
    *
    * Free pages in the normal zone are ignored; lowmem_reserve is assumed
    * to prohibit fallback from movable to normal zone.
    */
    free = mem_snap->movable_free_kb +
	    mem_snap->movable_inactive_file_kb;

    free += std::min(mem_snap->swap_free_kb, mem_snap->movable_inactive_anon_kb) *
            ZRAM_COMPRESSION_RATIO_PCT / 100;

    if (mem_snap->swap_free_kb && mem_snap->normal_free_kb > resolution * SIZE_1KB)
        free += (mem_snap->normal_free_kb);

    return free;
}

static inline int64_t get_movable_zspage_nr()
{
    char *buf;
    uint64_t movable_nr_zspages;

    pthread_mutex_lock(&fileread_buffer_mutex);
    buf = get_zoneinfo();
    if (!buf)
        goto err;

    if (parse_zone_field(buf, zone_names[ZONE_MOVABLE],
            "nr_zspages", &movable_nr_zspages))
        goto err;
    pthread_mutex_unlock(&fileread_buffer_mutex);

    return movable_nr_zspages;
err:
    LOG(ERROR) << "failed to get Movable zspage_nr";
    pthread_mutex_unlock(&fileread_buffer_mutex);

    return -EINVAL;
}

static inline uint64_t ceil_div_u64(uint64_t num, uint64_t den)
{
    return den ? (num + den - 1) / den : 0;
}

/*
 * number of blocks in Movable zone required to be plugged-in so that
 * zram pages will not be migrated out to Normal zone during unplug.
*/
static int64_t get_num_blocks_zram()
{
    uint64_t num_blocks;
    uint64_t total_bytes;
    int64_t nr_zspages;

    nr_zspages = get_movable_zspage_nr();
    if (nr_zspages <= 0)
        return nr_zspages;

    total_bytes = nr_zspages * (uint64_t)sys_page_size;
    num_blocks = ceil_div_u64(total_bytes, (uint64_t)resolution * SIZE_1MB);

    return num_blocks;
}

static uint64_t get_num_blocks_persistent()
{
    uint64_t min_blocks_persistent;
    int64_t num_blocks_zram;

    /* get min number of blocks required to host zram pages in Movable zone */
    num_blocks_zram = get_num_blocks_zram();

    min_blocks_persistent = ceil_div_u64((uint64_t)MIN_MOVABLE_SIZE_PERSISTENT_MB, resolution);

    /* adjust min_blocks_persistent to account for zram pages present */
    if (num_blocks_zram > 0)
        min_blocks_persistent = std::max((uint64_t)num_blocks_zram, min_blocks_persistent);

    return min_blocks_persistent;
}

static uint64_t adjust_count_to_remove(uint64_t count_to_remove)
{
    uint64_t max_removable, min_blocks_persistent;
    size_t total_blocks_plugged = 0;

    min_blocks_persistent = get_num_blocks_persistent();

    if(get_total_block_plugin_count(&total_blocks_plugged))
        return 0;

    /* keep atleast required number of blocks to be persistent */
    if (total_blocks_plugged <= min_blocks_persistent)
        return 0;

    max_removable = total_blocks_plugged - min_blocks_persistent;

    return (count_to_remove < max_removable)
               ? count_to_remove
               : max_removable;
}

static inline int new_pressure_event_received()
{
    if (get_atomic_variable_bool(cancel_check)) {
        set_atomic_variable_bool(cancel_check, false);
        set_atomic_variable_bool(wait_in_progress, false);
        return 1;
    }

    return 0;
}

static inline uint64_t get_movable_pgalloc_nr()
{
    char *buf;
    uint64_t pgalloc_movable_nr;

    pthread_mutex_lock(&fileread_buffer_mutex);
    buf = get_vmstat();
    if (!buf)
        goto err;

    if(parse_field(buf, "pgalloc_movable", &pgalloc_movable_nr))
        goto err;

    pthread_mutex_unlock(&fileread_buffer_mutex);
    return pgalloc_movable_nr;

err:
    LOG(ERROR) << "failed to get memory snapshot";
    pthread_mutex_unlock(&fileread_buffer_mutex);
    return 0;
}

/* Returns 1 if pgalloc_new > pgalloc_old by more than PGALLOC_GROWTH_THRESHOLD */
static inline int system_allocation_activity(uint64_t pgalloc_old, uint64_t pgalloc_new)
{
    if (pgalloc_old == 0)
        return 0;

    if (pgalloc_new <= pgalloc_old)
        return 0;

    return ((pgalloc_new - pgalloc_old) >= PGALLOC_GROWTH_THRESHOLD) ? 1 : 0;
}

/*
 * Adaptive idle wait: more blocks plugged means a heavier usecase is
 * running; wait longer before concluding it has ended.
 */
static uint64_t get_idle_wait_time_s()
{
    uint64_t chunks = mem_chunks_plugged.load();
    if (chunks >= 10) return IDLE_WAIT_TIME_S_HEAVY;
    if (chunks >= 5)  return IDLE_WAIT_TIME_S_MED;
    return IDLE_WAIT_TIME_S_BASE;
}

/*
 * Scale the post-removal hold-off window with the number of blocks
 * removed.  Each removal triggers kernel page migration, compaction and
 * zone-watermark updates that cause transient PSI stalls.  A larger
 * removal batch produces a longer stall storm.
 * Base: 3 s + 500 ms per block, capped at 8 s.
 */
static uint32_t get_holdoff_ms_after_remove(uint64_t blocks_removed)
{
    uint32_t ms = 3000 + (uint32_t)(blocks_removed * 500);
    return std::min(ms, (uint32_t)8000);
}

void* memtrack_thread_function(void *arg) {

    (void)(arg);
    struct timespec timeout;
    uint64_t count = 0, mem_chunks_unplugged = 0, total_free = 0;
    uint64_t kernel_count = 0, kernel_chunks_plugged = 0;
    struct memory_snapshot mem_snap;
    int res, retry_count;
    unsigned int wake_locked = 0;

    while (1) {

        /* lets wait for notify */
        wait_for_condition_timed(&thread_execution_cond,
                &thread_execution_mutex, NULL);
        retry_count = 0;

startover:
        mem_chunks_unplugged = 0;
        clock_gettime(CLOCK_MONOTONIC, &timeout);
        if (!retry_count)
            timeout.tv_sec += (time_t)get_idle_wait_time_s();
        else
            timeout.tv_sec += RETRY_WAIT_TIME_S;

        set_atomic_variable_bool(wait_in_progress, true);

        /* wait few seconds of idle in memory pressure */
        wait_for_condition_timed(&thread_execution_cond,
                &thread_execution_mutex, &timeout);

        /* check if we received any new memory pressure event during idle time wait */
        if (new_pressure_event_received())
            goto startover;

        /*
         * we have passed beyond some time of idle memory pressure.
         * its safe now to assume that no memory consuming usescases are running.
         */

        /* wait until pressure is decayed */
        if (enable_pressure_decay_wait)
            wait_until_pressure_decay();

        /* check if we received any new memory pressure event during pressure decay wait */
        if (new_pressure_event_received())
            goto startover;

        /* didn't receive any new memory pressure events, so time to release memory to PVM */

        /* take snapshot of memory */
        total_free = get_memsnap_and_total_free(&mem_snap);
        if (!total_free)
            goto retry;

        LOG(INFO) << "MemFree before UNPLUG: " << mem_snap.sys_memfree_kb <<
                " KB (Normal: " << mem_snap.normal_free_kb << " KB, Movable: " <<
                mem_snap.movable_free_kb << " KB, swap_free_kb: " << mem_snap.swap_free_kb << " KB)";

        LOG(INFO) << "Movable inactive_file: " << mem_snap.movable_inactive_file_kb <<
                        " KB : inactive_anon: " << mem_snap.movable_inactive_anon_kb << " KB";

        count = (total_free / SIZE_1KB) / resolution;

        /*
         * Integer floor division can zero out count even when there is a
         * meaningful reclaimable budget.  Example: total_free=1612 KB with
         * resolution=2048 KB gives count=0, yet 1 block is safely removable.
         * If the budget covers at least half a block (resolution/2 KB) and
         * there are blocks above the persistent floor, allow removing 1 block.
         * This prevents psi_daemon from getting permanently stuck at the
         * persistent floor + 1 block after boot or after a usecase ends.
         */
        if (count == 0 && total_free >= (resolution * SIZE_1KB / 2)) {
            uint64_t adj = adjust_count_to_remove(1);
            if (adj > 0) {
                count = 1;
                LOG(INFO) << "Partial budget (" << total_free / SIZE_1KB
                          << " KB < " << resolution << " MB): allowing 1 block unplug";
            }
        }

        if (!wake_locked && wake_lock_acquire())
            wake_locked = 1;

        /* update count to unplug based on minimum blocks system needs to have */
        count = adjust_count_to_remove(count);

        /* cap per-cycle removal to avoid large sudden memory drops that
         * trigger an immediate PSI storm and re-plugging */
        count = std::min(count, (uint64_t)MAX_BLOCKS_PER_UNPLUG_CYCLE);

        LOG(INFO) << "count_to_remove: " << count << " mem_chunks_plugged: " <<
                mem_chunks_plugged.load() << " resolution: " << resolution <<
                " MB plugged_memory: " << plugged_memory.load() << " MB";

        /* first release blocks added by kernel */
        if (get_kernel_plugin_count(&kernel_chunks_plugged) < 0) {
            LOG(ERROR) << "failed to get kernel plugin count";
            goto release_blocks;
        }
        LOG(INFO) << "kernel_chunks_plugged: " << kernel_chunks_plugged;

        kernel_count = std::min(count, kernel_chunks_plugged);

        /*** unplug memory blocks added by kernel ***/
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
        /*** unplug memory blocks added by psi-daemon service ***/
        count = std::min(count, mem_chunks_plugged.load());
        LOG(INFO) << "Count after kernel unplug: " << count;

        /*
         * Unplug one block at a time.  After each removal wait a short
         * settling period and re-evaluate whether more blocks should be
         * removed.  This prevents large sudden drops that trigger an
         * immediate PSI storm and re-plugging.
         */
        {
            uint64_t max_to_remove = count;
            while (max_to_remove-- > 0) {
                /* abort if a new pressure event arrived */
                if (new_pressure_event_received())
                    goto startover;

                res = memory_unplug_request(resolution);
                if (res) {
                    LOG(ERROR) << "Failed to unplug one memory chunk of "
                               << resolution << "MB";
                    continue;
                }

                clock_gettime(CLOCK_MONOTONIC, &last_remove_time);
                last_remove_holdoff_ms = get_holdoff_ms_after_remove(1);
                movable_pgalloc_last_unplug = get_movable_pgalloc_nr();
                mem_chunks_unplugged++;
                plugged_memory -= resolution;
                mem_chunks_plugged--;

                LOG(INFO) << "Unplugged 1 block (" << resolution
                          << " MB). Total plugged: " << plugged_memory.load() << " MB";

                if (max_to_remove == 0)
                    break;

                /*
                 * No inter-block sleep needed: the holdoff window set by
                 * last_remove_holdoff_ms already suppresses PSI events
                 * caused by the removal in psi_mainloop.  Just check for
                 * a new pressure event and continue immediately.
                 */
                if (new_pressure_event_received())
                    goto startover;

                /* re-evaluate: stop if memory is no longer surplus */
                total_free = get_memsnap_and_total_free(&mem_snap);
                if (!total_free)
                    break;
                uint64_t new_count = (total_free / SIZE_1KB) / resolution;
                new_count = adjust_count_to_remove(new_count);
                new_count = std::min(new_count, (uint64_t)MAX_BLOCKS_PER_UNPLUG_CYCLE);
                if (new_count == 0) {
                    LOG(INFO) << "Memory no longer surplus after removal, stopping";
                    break;
                }
                max_to_remove = std::min(max_to_remove, new_count - 1);
            }
        }

        if (mem_chunks_unplugged) {
            LOG(INFO) << "Unplugged " << mem_chunks_unplugged
                      << " memory chunks total. Total memory unplugged: "
                      << mem_chunks_unplugged * resolution << " MB";
            /* set holdoff scaled to total blocks removed this cycle */
            clock_gettime(CLOCK_MONOTONIC, &last_remove_time);
            last_remove_holdoff_ms = get_holdoff_ms_after_remove(mem_chunks_unplugged);
        }

        /* take memory snapshot after unplugging */
        if (system_memory_snapshot(&mem_snap))
            goto retry;

        LOG(DEBUG) << "MemFree after UNPLUG: " << mem_snap.sys_memfree_kb <<
                " KB (Normal: " << mem_snap.normal_free_kb << " KB, Movable: " <<
                mem_snap.movable_free_kb<< " KB)";

retry:
        if(get_total_block_plugin_count(&count))
            count = mem_chunks_plugged.load();

        /* if there are still blocks being plugged-in, retry and startover */
        if (count > get_num_blocks_persistent() && retry_count < MAX_UNPLUG_RETRY) {
            ++retry_count;
            /*
             * Only retry if there is actually free memory to reclaim.
             * If total_free is zero the budget formula already determined
             * the system is too tight to remove any block — retrying will
             * just spin MAX_UNPLUG_RETRY times doing nothing useful.
             * Break out now and wait for the next PSI wakeup to re-evaluate.
             */
            if (!total_free) {
                LOG(INFO) << "count_to_remove=0 and no free memory to reclaim. "
                             "Stopping retries, waiting for next PSI event.";
                retry_count = 0;
            } else {
                LOG(INFO) << "Retrying unplug after " << RETRY_WAIT_TIME_S <<
                        " seconds (retry attempt: " << retry_count << ")";
                goto startover;
            }
        }
        else {
            if (!total_free)
                LOG(INFO) << "No free memory left to unplug";

            if (retry_count == MAX_UNPLUG_RETRY)
                LOG(INFO) << "Max retry attempt reached for unplugging!!";

            if(get_total_block_plugin_count(&count))
                count = mem_chunks_plugged.load();
            if (count == 0)
                LOG(INFO) << "Unplugged all memory!!";
            retry_count = 0;
        }

        /*
         * Intentionally NOT doing goto startover here unconditionally.
         * If total_free==0 the system is genuinely tight — no amount of
         * retrying will free more blocks. The next PSI wakeup will
         * re-trigger the cycle when conditions change.
         */
        if(wake_locked && !wake_unlock())
            LOG (ERROR) << "failed to wake unlock";
        else
            wake_locked = 0;

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

static int64_t timespec_diff_ms(const struct timespec *a,
                                const struct timespec *b)
{
    int64_t sec_diff  = (int64_t)a->tv_sec - (int64_t)b->tv_sec;
    int64_t nsec_diff = (int64_t)a->tv_nsec - (int64_t)b->tv_nsec;

    return sec_diff * 1000 + nsec_diff / 1000000;
}

static inline bool within_remove_holdoff()
{
	struct timespec current;
	int64_t elapsed_ms;

    if (last_remove_time.tv_sec == 0 && last_remove_time.tv_nsec == 0) {
        // No prior remove; not within hold-off.
        return false;
    }

    clock_gettime(CLOCK_MONOTONIC, &current);
    elapsed_ms = timespec_diff_ms(&current, &last_remove_time);

    /* negative elapsed means clocks not monotonic */
    if (elapsed_ms < 0)
        return false;

    return (uint64_t)elapsed_ms < (uint64_t)last_remove_holdoff_ms;
}

static void psi_mainloop(void) {
    pressure_levels pressure_level = PRESSURE_NONE;
    struct timespec cur, start, end;
    struct memory_snapshot mem_snap;
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

        LOG(INFO) << "Received pressure event "<<
                psi_level_to_string(pressure_level) <<
                ". TIME: " << cur.tv_sec << "." << cur.tv_nsec/NS_PER_MS <<" s";

        if (system_memory_snapshot(&mem_snap))
            continue;

        LOG(INFO) << "MemFree: " << mem_snap.sys_memfree_kb <<
                " KB (Normal: " << mem_snap.normal_free_kb << " KB, Movable: " <<
                mem_snap.movable_free_kb << " KB)";

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
            (mem_snap.movable_free_kb < ((uint64_t)resolution * SIZE_1KB) / 2)) {    /* only if movable < 1/2 * resolution */

            /*
             * if we recently had a memblock remove, we would expect PSI pressure
             * to increase due to kernel doing migration, compaction, update zone
             * watermarks, takes required locks etc., which leads to temporary stalling
             * of processes leading to PSI events (PSI_SOME) being fired. This would
             * lead us here trying to plug a block again. This leads to "ping-pong"
             * effect of memory blocks being removed-added-removed again later.
             *
             * One reliable way to check for natural pressure vs PSI due to unplug
             * is to check for growth in pgalloc_movable allocations and having
             * a hold-off window after unplug to temporarily ignore PSI events
             * occuring due to unplug. If still within hold-off period and pgalloc_movable
             * has increased significantly, then its considered as PSI pressure due
             * to movable allocation activity, and we go ahead and plug-in memory block
             * irrespective of if we witin the remove holdoff period.
             */
            if (within_remove_holdoff() &&
                    !system_allocation_activity(movable_pgalloc_last_unplug, get_movable_pgalloc_nr())) {
                LOG(INFO) <<"PSI pressure within Remove Holdoff period. IGNORING PRESSURE EVEVNT...";
                continue;
            }

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
                continue;
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

            if (system_memory_snapshot(&mem_snap))
                continue;

            LOG(DEBUG) << "MemFree after PLUG: " << mem_snap.sys_memfree_kb <<
                    " KB (Normal: " << mem_snap.normal_free_kb << " KB, Movable: " <<
                    mem_snap.movable_free_kb<< " KB)";
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

static void notify_wakeup(void) {
    /* notify memtrack thread that we received new SIGUSR1 event to wakeup */
    LOG (INFO) << "Sending signal to memtrack thread to wakeup.";
    pthread_cond_signal(&thread_execution_cond);
}

/*
 * Seconds to wait after psi_daemon starts before firing the boot-settle
 * unplug cycle.  Boot PSI storms typically last 5-10s; 30s gives the
 * system enough time to fully settle before we attempt reclaim.
 */
#define BOOT_SETTLE_WAIT_S  10

/*
 * boot_settle_thread — fires one guaranteed unplug cycle after boot.
 *
 * Problem: after the boot PSI storm, the system may go quiet with no
 * further PSI events.  memtrack_thread_function only wakes on
 * pthread_cond_signal from psi_mainloop, so if no PSI event arrives
 * after boot the plugged blocks are never reclaimed — until an external
 * wakeup (user login, adb) happens to resume the system.
 *
 * Fix: sleep BOOT_SETTLE_WAIT_S seconds then send one cond_signal to
 * guarantee memtrack runs at least once after boot has settled.
 * After that this thread exits — it is a one-shot.
 */
void* boot_settle_thread(void* arg) {
    (void)arg;

    LOG(INFO) << "boot_settle_thread: waiting " << BOOT_SETTLE_WAIT_S
              << "s for system to settle after boot...";

    sleep(BOOT_SETTLE_WAIT_S);

    LOG(INFO) << "boot_settle_thread: boot settle wait done, "
                 "triggering initial unplug cycle.";
    pthread_cond_signal(&thread_execution_cond);

    return NULL;
}

void* signal_thread(void* arg) {

    (void)arg;
    int sig, ret;
    sigset_t signal;

    sigemptyset(&signal);
    sigaddset(&signal, SIGUSR1);

    /* ensure SIGUSR1 is blocked in this thread too (should already be via main) */
    pthread_sigmask(SIG_BLOCK, &signal, NULL);

    for (;;) {

        /* wait for SIGUSR1 signal. blocks until SIGUSR1 is pending */
        ret = sigwait(&signal, &sig);

        if (ret == 0 && sig == SIGUSR1) {
            LOG(INFO) << "Received SIGUSR1 event";
            /* notify reclaim thread to wakeup */
            notify_wakeup();
        }
    }

    return NULL;
}

int main(void) {

    int i;
    pthread_t memtrack_thread, sigusr1_thread, boot_settle_thread_id;
    std::string thresholds;
    char str[LINE_MAX];
    struct memory_snapshot mem_snap;
    size_t kernel_count;
    sigset_t signal;

    sigemptyset(&signal);
    sigaddset(&signal, SIGUSR1);

    /* Block SIGUSR1 in the main thread before creating any threads. */
    if (pthread_sigmask(SIG_BLOCK, &signal, NULL) != 0) {
        LOG(ERROR) << "pthread_sigmask failed";
        return -EINVAL;
    }

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

    /* create pthread for listening to SIGUSR1 events from userspace */
    if (pthread_create(&sigusr1_thread, NULL, &signal_thread, NULL)) {
        LOG(ERROR) << "Error creating pthread for SIGUSR1 event tracking";
        return -EINVAL;
    }

    /* create one-shot thread to trigger unplug after boot settles */
    if (pthread_create(&boot_settle_thread_id, NULL, &boot_settle_thread, NULL)) {
        LOG(ERROR) << "Error creating boot_settle_thread — boot-time unplug may not run";
    } else {
        pthread_detach(boot_settle_thread_id);
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

    /* reclaim any memory blocks added by kernel */
    if (get_kernel_plugin_count(&kernel_count) < 0) {
        LOG(ERROR) << "failed to get kernel plugin count";
        return -EINVAL;
    }
    LOG(INFO) << "kernel_count during psi_daemon bootup: " << kernel_count;
    memory_unplug_request_kernel(kernel_count);

    if (system_memory_snapshot(&mem_snap))
        return -EINVAL;

    LOG(INFO) << "MemFree : " << mem_snap.sys_memfree_kb <<
            " KB (Normal: " << mem_snap.normal_free_kb << " KB, Movable: " <<
            mem_snap.movable_free_kb<< " KB)";

    LOG(INFO) << "Waiting for pressure events...";
    psi_mainloop();

    /* should not exit */
    LOG(ERROR) << "Exiting...";

    memory_plug_deinit();
    return 0;
}
