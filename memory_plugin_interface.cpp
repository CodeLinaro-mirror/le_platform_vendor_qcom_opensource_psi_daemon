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

using namespace std;

/* mem_buf fds returned by virtio-mem driver */
static vector<int> array_memfd;

#if defined(QTI_VIRTIO_MEM_IOC_HINT_CREATE)
int virtio_mem_plug_memory(int64_t size, const std::string& name)
{
    struct qti_virtio_mem_ioc_hint_create_arg arg = {};
    int virtio_mem_fd, ret;

    virtio_mem_fd = TEMP_FAILURE_RETRY(open(kVirtioMemPath, O_RDONLY | O_CLOEXEC));
    if (virtio_mem_fd < 0) {
        LOG(ERROR) << "Unable to open " << kVirtioMemPath << " : " << strerror(errno) << "\n";
        return virtio_mem_fd;
    }

    arg.size = size;
    strlcpy(arg.name, name.c_str(), sizeof(arg.name));

    ret = ioctl(virtio_mem_fd, QTI_VIRTIO_MEM_IOC_HINT_CREATE, &arg);
    if (ret) {
        LOG(ERROR) << "MemorySizeHint() Failed.\n";
        close(virtio_mem_fd);
        return ret;
    }

    close(virtio_mem_fd);
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

//TODO: get these info after querying from qcom virtio-mem driver */

/* memory plugin size defaults (in MBs)*/
#define DEFAULT_PLUGIN_RESOLUTION_MB    (4)
#define DEFAULT_MAX_MEMORY_PLUGIN_MB    (256)

int64_t get_memory_plugin_resolution(void) {
    return DEFAULT_PLUGIN_RESOLUTION_MB;
}

int64_t get_max_memory_plugin_allowed(void) {
    return DEFAULT_MAX_MEMORY_PLUGIN_MB;
}

int memory_unplug_all_request(void) {
    uint64_t initial_count, unplugged_count = 0, res;
    uint64_t resolution = DEFAULT_PLUGIN_RESOLUTION_MB;

    initial_count = array_memfd.size();
    if (!initial_count) {
        LOG(ERROR) << "No memory available to unplug";
        return 0;
    }

    while (array_memfd.size()) {
        LOG(INFO) << "releasing one memory chunk" << resolution <<" MB to host (PVM)";
        res = close(array_memfd.back());
        array_memfd.pop_back();
        if (res)
            LOG(ERROR) << "Failed to unplug one memory chunk of size "<<
                resolution <<" MB";
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

