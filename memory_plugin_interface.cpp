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

#include "vmmem.h"
#include "vmmem_wrapper.h"

#define SIZE_1MB        0x00100000

static char psi_daemon_name[] = "psi_daemon";

using namespace std;

/* vmmem interface handle */
static VmMem *vmmem;

/* mem_buf fds returned by vmmem interface */
static vector<int> array_memfd;

int memory_plug_request(uint64_t size) {
    int memfd;

    vmmem = CreateVmMem();
    if (!vmmem) {
        LOG(ERROR) << "CreateVmMem failed";
        return -1;
    }

    memfd = MemorySizeHint(vmmem, size * SIZE_1MB, psi_daemon_name);
    if (memfd < 0) {
        LOG(ERROR) << "failed to suggest memory size hint";
        FreeVmMem(vmmem);
        return -1;
    }

    LOG(INFO) << "Memory of size "<< size <<" MB plugged-in successfully";
    array_memfd.push_back(memfd);

    FreeVmMem(vmmem);
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
#define DEFAULT_PLUGIN_RESOLUTION_MB    (16)
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

