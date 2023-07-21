/*
 * Copyright (c) 2023 Qualcomm Innovation Center, Inc. All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause-Clear
 *
 * Memory plugin request interfaces for psi_daemon.
 */

#define LOG_TAG "mem_plugin_interface"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <vector>
#include <base/logging.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/qti_virtio_mem.h>
#include <cutils/memory.h>

#define SIZE_1MB        0x00100000

static char psi_daemon_name[] = "psi_daemon";
static constexpr char kVirtioMemPath[] = "/dev/qti_virtio_mem";
static int virtio_mem_fd = -1;

#define QVM_SYS_DEVICE_PATH         "/sys/devices/virtual/qti_virtio_mem/qti_virtio_mem"
#define QVM_BLOCK_SIZE_PATH         QVM_SYS_DEVICE_PATH"/device_block_size"
#define QVM_MAX_PLUGIN_THRES_PATH   QVM_SYS_DEVICE_PATH"/max_plugin_threshold"
#define QVM_NUM_BLOCK_PLUGGED_PATH  QVM_SYS_DEVICE_PATH"/device_block_plugged"
#define QVM_NUM_KERNEL_PLUGGED_PATH QVM_SYS_DEVICE_PATH"/kernel_plugged"
#define QVM_NUM_KERNEL_UNPLUG_PATH  QVM_SYS_DEVICE_PATH"/kernel_unplug"

char *read_file(const char *file_path);
int write_file(const char *file_path, char *s);

#define LINE_MAX 250

using namespace std;

/* mem_buf fds returned by virtio-mem driver */
static vector<int> array_memfd;

#if defined(QTI_VIRTIO_MEM_IOC_HINT_CREATE)
int virtio_mem_plug_memory(int64_t size, const std::string& name)
{
    struct qti_virtio_mem_ioc_hint_create_arg arg = {};
    int ret;

    if (virtio_mem_fd < 0)
        return -ENOTTY;

    arg.size = size;
    strlcpy(arg.name, name.c_str(), sizeof(arg.name));

    ret = ioctl(virtio_mem_fd, QTI_VIRTIO_MEM_IOC_HINT_CREATE, &arg);
    if (ret) {
        LOG(ERROR) << "MemorySizeHint() Failed.\n";
        return ret;
    }

    return arg.fd;
}
#else
int virtio_mem_plug_memory(int64_t size, const std::string& name)
{
    (void)size;
    (void)name;

    LOG(ERROR) << "MemorySizeHint() NOT SUPPORTED.\n";
    return -ENOTTY;
}
#endif

int memory_plug_init() {

    virtio_mem_fd = TEMP_FAILURE_RETRY(open(kVirtioMemPath, O_RDONLY | O_CLOEXEC));
    if (virtio_mem_fd < 0) {
        LOG(ERROR) << "Unable to open " << kVirtioMemPath << " : " << strerror(errno) << "\n";
        return errno;
    }

    return 0;
}

void memory_plug_deinit() {
    if (virtio_mem_fd >= 0)
        close(virtio_mem_fd);
}

int memory_plug_request(uint64_t size) {
    int memfd;

    memfd = virtio_mem_plug_memory(size * SIZE_1MB, psi_daemon_name);
    if (memfd < 0) {
        LOG(ERROR) << "failed to suggest memory size hint";
        return -1;
    }

    LOG(INFO) << "Memory of size "<< size <<" MB plugged-in successfully";
    array_memfd.push_back(memfd);

    return 0;
}

int memory_unplug_request(uint64_t size) {
    int res;

    if (array_memfd.size()) {
        res = close(array_memfd.back());
        array_memfd.pop_back();
        if (res)
            LOG(ERROR) << "Failed to unplug one memory chunk of size "<< size <<" MB ";

        return res;
    }

    LOG(ERROR) << "No memory available to unplug";
    return -ENOTTY;
}

int get_memory_plugin_resolution(uint64_t *plugin_resolution_mb) {
    char *buf;

    buf = read_file(QVM_BLOCK_SIZE_PATH);
    if (!buf)
        return -EINVAL;

    *plugin_resolution_mb = strtoul(buf, 0, 10);
    *plugin_resolution_mb /= SIZE_1MB;

    return 0;
}

int get_max_memory_plugin_allowed(uint64_t *max_memory_plugin_mb) {
    char *buf;

    buf = read_file(QVM_MAX_PLUGIN_THRES_PATH);
    if (!buf)
        return -EINVAL;

    *max_memory_plugin_mb = strtoul(buf, 0, 10);
    *max_memory_plugin_mb /= SIZE_1MB;

    return 0;
}

int get_kernel_plugin_count(size_t *count) {
    char *buf;

    buf = read_file(QVM_NUM_KERNEL_PLUGGED_PATH);
    if (!buf)
        return -EINVAL;

    *count = strtoul(buf, 0, 10);
    return 0;
}

int memory_unplug_request_kernel(size_t count) {
    char str_val[LINE_MAX];

    snprintf(str_val, sizeof(str_val), "%lu", count);
    if (write_file(QVM_NUM_KERNEL_UNPLUG_PATH, str_val)) {
        LOG(ERROR) << "Failed to write to " << QVM_NUM_KERNEL_UNPLUG_PATH;
        return -EINVAL;
    }

    get_kernel_plugin_count(&count);
    LOG(INFO) << "Num kernel blocks plugin after write: " << count;
    return 0;
}

int memory_unplug_all_request(void) {
    uint64_t initial_count, unplugged_count = 0, res;

    initial_count = array_memfd.size();
    if (!initial_count) {
        LOG(ERROR) << "No memory available to unplug";
        return 0;
    }

    while (array_memfd.size()) {
        LOG(INFO) << "releasing one memory chunk to host (PVM)";
        res = close(array_memfd.back());
        array_memfd.pop_back();
        if (res)
            LOG(ERROR) << "Failed to unplug one memory chunk";
        else
            unplugged_count++;
    }

    if (unplugged_count < initial_count)
        LOG(INFO) << "not all memory chunks were unplugged. initial_count: "<<
            initial_count <<" unplugged_count: "<< unplugged_count;
    else
        LOG(INFO) << "Successfully unplugged all memory chunks. unplugged_count: "<<
            unplugged_count;

    return unplugged_count;
}

