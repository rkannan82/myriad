package com.ebay.myriad.scheduler;

import com.ebay.myriad.executor.ContainerTaskStatusRequest;
import com.ebay.myriad.scheduler.yarn.interceptor.BaseInterceptor;
import com.ebay.myriad.scheduler.yarn.interceptor.InterceptorRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class YarnNodeCapacityManager extends BaseInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(YarnNodeCapacityManager.class);
    private final AbstractYarnScheduler yarnScheduler;
    private final MyriadDriver myriadDriver;

    // TODO(Santosh): Define a single class that encapsulates (Offer, SchedulerNode, containersBeforeHB)
    private Map<String, Protos.Offer> offersMap = new ConcurrentHashMap<>(200, 0.75f, 50);
    private Map<String, SchedulerNode> schedulerNodes = new ConcurrentHashMap<>(200, 0.75f, 50);
    private Map<NodeId, Set<RMContainer>> containersBeforeHB = new ConcurrentHashMap<>(200, 0.75f, 50);
    private Map<String, Protos.SlaveID> slaves = new ConcurrentHashMap<>(200, 0.75f, 50);

    @Inject
    public YarnNodeCapacityManager(InterceptorRegistry registry, AbstractYarnScheduler yarnScheduler, MyriadDriver myriadDriver) {
        if (registry != null) {
            registry.register(this);
        }
        this.yarnScheduler = yarnScheduler;
        this.myriadDriver = myriadDriver;
    }

    public void addResourceOffers(List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if (schedulerNodes.containsKey(offer.getHostname())) {
                offersMap.put(offer.getHostname(), offer);
                slaves.put(offer.getHostname(), offer.getSlaveId());
            } else {
                myriadDriver.getDriver().declineOffer(offer.getId());
            }
        }
    }

    @Override
    public void beforeRMNodeEventHandled(RMNodeEvent event, RMContext context) {
        switch (event.getType()) {
            case STARTED: {
                // TODO(Santosh): We can't zero out resources here in all cases. For e.g.
                // this event might be fired when an existing node is rejoining the
                // cluster (NM restart) as well. Sometimes this event can have a list of
                // container statuses, which, capacity/fifo schedulers seem to handle
                // (perhaps due to work preserving NM restart).
                RMNode addedRMNode = context.getRMNodes().get(event.getNodeId());
                addedRMNode.setResourceOption(
                    ResourceOption.newInstance(Resource.newInstance(0, 0),
                        RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT));
            }
            break;

            case EXPIRE: {
                schedulerNodes.remove(event.getNodeId().getHost());
            }
            break;

            case STATUS_UPDATE: {
                RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;
                RMNode rmNode = context.getRMNodes().get(event.getNodeId());
                SchedulerNode schedulerNode = yarnScheduler.getSchedulerNode(rmNode.getNodeID());
                String hostName = rmNode.getNodeID().getHost();
                Protos.Offer offer = offersMap.get(hostName);

                int rmSchNodeNumContainers = schedulerNode.getNumContainers();

                List<ContainerStatus> nmContainers = statusEvent.getContainers();

                if (nmContainers.size() != rmSchNodeNumContainers) {
                    LOGGER.warn("Node: {}, Num Containers known by RM scheduler: {}, Num containers NM is reporting: {}",
                        hostName, rmSchNodeNumContainers, nmContainers.size());
                }

                Resource nmFreed = Resource.newInstance(0, 0);
                Resource nmUnderUse = Resource.newInstance(0, 0);
                for (ContainerStatus status : nmContainers) {
                    ContainerId containerId = status.getContainerId();
                    Resource allocatedResource = yarnScheduler.getRMContainer(containerId).getAllocatedResource();
                    if (status.getState() == ContainerState.COMPLETE) {
                        Resources.addTo(nmFreed, allocatedResource);
                        requestExecutorToSendTaskStatusUpdate(slaves.get(hostName),
                            containerId, Protos.TaskState.TASK_FINISHED);
                    } else { // state == NEW | RUNNING
                        Resources.addTo(nmUnderUse, allocatedResource);
                        requestExecutorToSendTaskStatusUpdate(slaves.get(hostName),
                            containerId, Protos.TaskState.TASK_RUNNING);
                    }
                }

                Resource mesosOffer = getResourceFromOffer(offer);
                // capacity of NM at this moment = (mesos offer) + (nmUnderUse) - (nmFreed)
                // this may take the capacity to negative if mesos offer is zero. it is ok
                // because when we set this 'new node capacity' into RMNode, the scheduler first
                // picks the negative value, then adds the 'nmFreed' resources to the node capacity.
                // So, finally after processing this HB, the capacity of the node as known by the scheduler
                // will be equal to 'nmUnderUse'.
                Resource nmNewCapacity = Resources.subtract(Resources.add(nmUnderUse, mesosOffer), nmFreed);
                rmNode.setResourceOption(ResourceOption.newInstance(
                    nmNewCapacity, RMNode.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT));
                containersBeforeHB.put(rmNode.getNodeID(), new HashSet<>(
                    schedulerNode.getRunningContainers()));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("(Before HB Handled) Node: {}, NM Freed: {}, Under NM's use: {}, " +
                            "Mesos Offer: {}, New NM Capacity: {}", hostName, nmFreed.toString(),
                        nmUnderUse.toString(), mesosOffer.toString(), nmNewCapacity.toString());
                }
            }
            break;
        }
    }

    @Override
    public void afterSchedulerEventHandled(SchedulerEvent event) {
        switch (event.getType()) {
            case NODE_ADDED: {
                NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
                String host = nodeAddedEvent.getAddedRMNode().getNodeID().getHost();
                if (schedulerNodes.containsKey(host)) {
                    LOGGER.warn("Duplicate node being added. Host: {}", host);
                }
                schedulerNodes.put(host, yarnScheduler.getSchedulerNode(nodeAddedEvent.getAddedRMNode().getNodeID()));
            }
            break;

            case NODE_UPDATE: {
                RMNode rmNode = ((NodeUpdateSchedulerEvent) event).getRMNode();
                String host = rmNode.getNodeID().getHost();

                Set<RMContainer> containersBeforeHB = this.containersBeforeHB.get(rmNode.getNodeID());
                Set<RMContainer> containersAfterHB = new HashSet<>(yarnScheduler.getSchedulerNode(
                    rmNode.getNodeID()).getRunningContainers());
                Set<RMContainer> containersAllocatedDueToMesosOffer =
                    Sets.difference(containersAfterHB, containersBeforeHB).immutableCopy();

                Protos.Offer offer = offersMap.get(host);
                if (containersAllocatedDueToMesosOffer.isEmpty()) {
                    if (offer != null) {
                        myriadDriver.getDriver().declineOffer(offer.getId());
                    }
                } else {
                    List<Protos.TaskInfo> tasks = Lists.newArrayList();
                    for (RMContainer newContainer : containersAllocatedDueToMesosOffer) {
                        tasks.add(getTaskInfoForContainer(newContainer, offer.getSlaveId()));
                    }
                    myriadDriver.getDriver().launchTasks(offer.getId(), tasks);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Launched containers: {} for offer: {}",
                            containersAllocatedDueToMesosOffer.toString(),
                            offersMap.get(host).getId().getValue());
                    }
                    offersMap.remove(host);
                }
            }
            break;
        }
    }

    /** sends a request to executor on the given slave to send back a status update
     * for the task launched for this container.
     *
     * TODO(Santosh): An aux-service on the NM might be a better way to do this.
     *
     * @param slaveId
     * @param containerId
     * @param taskState
     */
    private void requestExecutorToSendTaskStatusUpdate(Protos.SlaveID slaveId,
                                                       ContainerId containerId,
                                                       Protos.TaskState taskState) {
        ContainerTaskStatusRequest containerTaskStatusRequest = new ContainerTaskStatusRequest();
        containerTaskStatusRequest.setMesosTaskId(
            ContainerTaskStatusRequest.YARN_CONTAINER_TASK_ID_PREFIX + containerId.toString());
        containerTaskStatusRequest.setState(taskState.name());
        myriadDriver.getDriver().sendFrameworkMessage(
            getExecutorId(slaveId),
            slaveId,
            new Gson().toJson(containerTaskStatusRequest).getBytes());
    }

    private Protos.ExecutorID getExecutorId(Protos.SlaveID slaveId) {
        return Protos.ExecutorID.newBuilder().setValue(
            TaskFactory.NMTaskFactoryImpl.EXECUTOR_PREFIX + slaveId.getValue()).build();
    }

    private Resource getResourceFromOffer(Protos.Offer offer) {
        int cpus = 0;
        int mem = 0;
        if (offer != null) {
            for (Protos.Resource resource : offer.getResourcesList()) {
                if (resource.getName().equalsIgnoreCase("cpus")) {
                    cpus += Math.round(resource.getScalar().getValue());
                } else if (resource.getName().equalsIgnoreCase("mem")) {
                    mem += Math.round(resource.getScalar().getValue());
                }
            }
        }
        return Resource.newInstance(mem, cpus);
    }

    private Protos.TaskInfo getTaskInfoForContainer(RMContainer rmContainer, Protos.SlaveID slaveId) {
        Container container = rmContainer.getContainer();
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
            .setValue(ContainerTaskStatusRequest.YARN_CONTAINER_TASK_ID_PREFIX + container.getId().toString()).build();

        return Protos.TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(slaveId)
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(container.getResource().getVirtualCores())))
            .addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(container.getResource().getMemory())))
//            .setExecutor(Protos.ExecutorInfo.newBuilder(nmExecutorInfo))
            .build();
    }
}
