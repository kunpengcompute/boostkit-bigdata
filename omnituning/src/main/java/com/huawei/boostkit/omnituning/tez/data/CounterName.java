/*
 * Copyright (C) 2020-2023. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.omnituning.tez.data;

import java.util.HashMap;
import java.util.Map;

public enum CounterName {
    NUM_SUCCEEDED_TASKS(GroupName.TezDag, "NUM_SUCCEEDED_TASKS", "NUM_SUCCEEDED_TASKS"),
    TOTAL_LAUNCHED_TASKS(GroupName.TezDag, "TOTAL_LAUNCHED_TASKS", "TOTAL_LAUNCHED_TASKS"),
    RACK_LOCAL_TASKS(GroupName.TezDag, "RACK_LOCAL_TASKS", "RACK_LOCAL_TASKS"),
    AM_CPU_MILLISECONDS(GroupName.TezDag, "AM_CPU_MILLISECONDS", "AM_CPU_MILLISECONDS"),
    AM_GC_TIME_MILLIS(GroupName.TezDag, "AM_GC_TIME_MILLIS", "AM_GC_TIME_MILLIS"),
    FILE_BYTES_READ(GroupName.FileSystemCounters, "FILE_BYTES_READ", "FILE_BYTES_READ"),
    FILE_BYTES_WRITTEN(GroupName.FileSystemCounters, "FILE_BYTES_WRITTEN", "FILE_BYTES_WRITTEN"),
    FILE_READ_OPS(GroupName.FileSystemCounters, "FILE_READ_OPS", "FILE_READ_OPS"),
    FILE_LARGE_READ_OPS(GroupName.FileSystemCounters, "FILE_LARGE_READ_OPS", "FILE_LARGE_READ_OPS"),
    FILE_WRITE_OPS(GroupName.FileSystemCounters, "FILE_WRITE_OPS", "FILE_WRITE_OPS"),
    HDFS_BYTES_READ(GroupName.FileSystemCounters, "HDFS_BYTES_READ", "HDFS_BYTES_READ"),
    HDFS_BYTES_WRITTEN(GroupName.FileSystemCounters, "HDFS_BYTES_WRITTEN", "HDFS_BYTES_WRITTEN"),
    HDFS_READ_OPS(GroupName.FileSystemCounters, "HDFS_READ_OPS", "HDFS_READ_OPS"),
    HDFS_LARGE_READ_OPS(GroupName.FileSystemCounters, "HDFS_LARGE_READ_OPS", "HDFS_LARGE_READ_OPS"),
    HDFS_WRITE_OPS(GroupName.FileSystemCounters, "HDFS_WRITE_OPS", "HDFS_WRITE_OPS"),
    S3_BYTES_READ(GroupName.FileSystemCounters, "S3_BYTES_READ", "S3_BYTES_READ"),
    S3_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3_BYTES_WRITTEN", "S3_BYTES_WRITTEN"),
    S3_READ_OPS(GroupName.FileSystemCounters, "S3_READ_OPS", "S3_READ_OPS"),
    S3_LARGE_READ_OPS(GroupName.FileSystemCounters, "S3_LARGE_READ_OPS", "S3_LARGE_READ_OPS"),
    S3_WRITE_OPS(GroupName.FileSystemCounters, "S3_WRITE_OPS", "S3_WRITE_OPS"),
    S3A_BYTES_READ(GroupName.FileSystemCounters, "S3A_BYTES_READ", "S3A_BYTES_READ"),
    S3A_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3A_BYTES_WRITTEN", "S3A_BYTES_WRITTEN"),
    S3A_READ_OPS(GroupName.FileSystemCounters, "S3A_READ_OPS", "S3A_READ_OPS"),
    S3A_LARGE_READ_OPS(GroupName.FileSystemCounters, "S3A_LARGE_READ_OPS", "S3A_LARGE_READ_OPS"),
    S3A_WRITE_OPS(GroupName.FileSystemCounters, "S3A_WRITE_OPS", "S3A_WRITE_OPS"),
    S3N_BYTES_READ(GroupName.FileSystemCounters, "S3N_BYTES_READ", "S3N_BYTES_READ"),
    S3N_BYTES_WRITTEN(GroupName.FileSystemCounters, "S3N_BYTES_WRITTEN", "S3N_BYTES_WRITTEN"),
    S3N_READ_OPS(GroupName.FileSystemCounters, "S3N_READ_OPS", "S3N_READ_OPS"),
    S3N_LARGE_READ_OPS(GroupName.FileSystemCounters, "S3N_LARGE_READ_OPS", "S3N_LARGE_READ_OPS"),
    S3N_WRITE_OPS(GroupName.FileSystemCounters, "S3N_WRITE_OPS", "S3N_WRITE_OPS"),
    REDUCE_INPUT_GROUPS(GroupName.TezTask, "REDUCE_INPUT_GROUPS", "REDUCE_INPUT_GROUPS"),
    REDUCE_INPUT_RECORDS(GroupName.TezTask, "REDUCE_INPUT_RECORDS", "REDUCE_INPUT_RECORDS"),
    COMBINE_INPUT_RECORDS(GroupName.TezTask, "COMBINE_INPUT_RECORDS", "COMBINE_INPUT_RECORDS"),
    SPILLED_RECORDS(GroupName.TezTask, "SPILLED_RECORDS", "SPILLED_RECORDS"),
    NUM_SHUFFLED_INPUTS(GroupName.TezTask, "NUM_SHUFFLED_INPUTS", "NUM_SHUFFLED_INPUTS"),
    NUM_SKIPPED_INPUTS(GroupName.TezTask, "NUM_SKIPPED_INPUTS", "NUM_SKIPPED_INPUTS"),
    NUM_FAILED_SHUFFLE_INPUTS(GroupName.TezTask, "NUM_FAILED_SHUFFLE_INPUTS", "NUM_FAILED_SHUFFLE_INPUTS"),
    MERGED_MAP_OUTPUTS(GroupName.TezTask, "MERGED_MAP_OUTPUTS", "MERGED_MAP_OUTPUTS"),
    GC_TIME_MILLIS(GroupName.TezTask, "GC_TIME_MILLIS", "GC_TIME_MILLIS"),
    COMMITTED_HEAP_BYTES(GroupName.TezTask, "COMMITTED_HEAP_BYTES", "COMMITTED_HEAP_BYTES"),
    INPUT_RECORDS_PROCESSED(GroupName.TezTask, "INPUT_RECORDS_PROCESSED", "INPUT_RECORDS_PROCESSED"),
    OUTPUT_RECORDS(GroupName.TezTask, "OUTPUT_RECORDS", "OUTPUT_RECORDS"),
    OUTPUT_BYTES(GroupName.TezTask, "OUTPUT_BYTES", "OUTPUT_BYTES"),
    OUTPUT_BYTES_WITH_OVERHEAD(GroupName.TezTask, "OUTPUT_BYTES_WITH_OVERHEAD", "OUTPUT_BYTES_WITH_OVERHEAD"),
    OUTPUT_BYTES_PHYSICAL(GroupName.TezTask, "OUTPUT_BYTES_PHYSICAL", "OUTPUT_BYTES_PHYSICAL"),
    ADDITIONAL_SPILLS_BYTES_WRITTEN(GroupName.TezTask, "ADDITIONAL_SPILLS_BYTES_WRITTEN", "ADDITIONAL_SPILLS_BYTES_WRITTEN"),
    ADDITIONAL_SPILLS_BYTES_READ(GroupName.TezTask, "ADDITIONAL_SPILLS_BYTES_READ", "ADDITIONAL_SPILLS_BYTES_READ"),
    ADDITIONAL_SPILL_COUNT(GroupName.TezTask, "ADDITIONAL_SPILL_COUNT", "ADDITIONAL_SPILL_COUNT"),
    SHUFFLE_BYTES(GroupName.TezTask, "SHUFFLE_BYTES", "SHUFFLE_BYTES"),
    SHUFFLE_BYTES_DECOMPRESSED(GroupName.TezTask, "SHUFFLE_BYTES_DECOMPRESSED", "SHUFFLE_BYTES_DECOMPRESSED"),
    SHUFFLE_BYTES_TO_MEM(GroupName.TezTask, "SHUFFLE_BYTES_TO_MEM", "SHUFFLE_BYTES_TO_MEM"),
    SHUFFLE_BYTES_TO_DISK(GroupName.TezTask, "SHUFFLE_BYTES_TO_DISK", "SHUFFLE_BYTES_TO_DISK"),
    SHUFFLE_BYTES_DISK_DIRECT(GroupName.TezTask, "SHUFFLE_BYTES_DISK_DIRECT", "SHUFFLE_BYTES_DISK_DIRECT"),
    NUM_MEM_TO_DISK_MERGES(GroupName.TezTask, "NUM_MEM_TO_DISK_MERGES", "NUM_MEM_TO_DISK_MERGES"),
    CPU_MILLISECONDS(GroupName.TezTask,"CPU_MILLISECONDS","CPU_MILLISECONDS"),
    PHYSICAL_MEMORY_BYTES(GroupName.TezTask,"PHYSICAL_MEMORY_BYTES","PHYSICAL_MEMORY_BYTES"),
    VIRTUAL_MEMORY_BYTES(GroupName.TezTask,"VIRTUAL_MEMORY_BYTES","VIRTUAL_MEMORY_BYTES"),
    NUM_DISK_TO_DISK_MERGES(GroupName.TezTask, "NUM_DISK_TO_DISK_MERGES", "NUM_DISK_TO_DISK_MERGES"),
    SHUFFLE_PHASE_TIME(GroupName.TezTask, "SHUFFLE_PHASE_TIME", "SHUFFLE_PHASE_TIME"),
    MERGE_PHASE_TIME(GroupName.TezTask, "MERGE_PHASE_TIME", "MERGE_PHASE_TIME"),
    FIRST_EVENT_RECEIVED(GroupName.TezTask, "FIRST_EVENT_RECEIVED", "FIRST_EVENT_RECEIVED"),
    LAST_EVENT_RECEIVED(GroupName.TezTask, "LAST_EVENT_RECEIVED", "LAST_EVENT_RECEIVED");

    static final Map<String, CounterName> counterDisplayNameMap;
    static final Map<String, CounterName> counterNameMap;

    static {
        counterDisplayNameMap = new HashMap<>();
        counterNameMap = new HashMap<>();
        for (CounterName cn : CounterName.values()) {
            counterDisplayNameMap.put(cn.displayName, cn);
            counterNameMap.put(cn.name, cn);
        }
    }

    final GroupName group;
    final String name;
    final String displayName;

    CounterName(GroupName group, String name, String displayName) {
        this.group = group;
        this.name = name;
        this.displayName = displayName;
    }

    public static CounterName getCounterFromName(String name) {
        if (counterNameMap.containsKey(name)) {
            return counterNameMap.get(name);
        }
        return null;
    }

    public static CounterName getCounterFromDisplayName(String displayName) {
        if (counterDisplayNameMap.containsKey(displayName)) {
            return counterDisplayNameMap.get(displayName);
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getGroupName() {
        return group.name();
    }
}
