package com.ebay.myriad.executor;

import org.apache.mesos.Protos;

public class ContainerTaskStatusRequest {
    public static final String YARN_CONTAINER_TASK_ID_PREFIX = "yarn_";
    private String mesosTaskId; // YARN_CONTAINER_TASK_ID_PREFIX + <container_id>
    private String state; // Protos.TaskState.name()

    public String getMesosTaskId() {
        return mesosTaskId;
    }

    public void setMesosTaskId(String mesosTaskId) {
        this.mesosTaskId = mesosTaskId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
