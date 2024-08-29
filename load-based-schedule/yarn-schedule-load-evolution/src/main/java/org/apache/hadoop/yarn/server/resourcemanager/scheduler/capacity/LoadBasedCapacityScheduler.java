package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

public class LoadBasedCapacityScheduler extends CapacityScheduler {
    public LoadBasedCapacityScheduler() {
        super(LoadBasedCapacityScheduler.class.getName());
    }
}