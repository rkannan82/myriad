package com.ebay.myriad.scheduler;

import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.configuration.NodeManagerConfiguration;
import com.ebay.myriad.executor.NMTaskConfig;
import com.ebay.myriad.state.NodeTask;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Value.Scalar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Objects;

public interface TaskFactory {
    TaskInfo createTask(Offer offer, TaskID taskId, NodeTask nodeTask);

    // TODO(Santosh): This is needed because the ExecutorInfo constructed
    // to launch NM needs to be specified to launch placeholder tasks for
    // yarn containers (for fine grained scaling).
    // If mesos supports just specifying the 'ExecutorId' without the full
    // ExecutorInfo, we wouldn't need this interface method.
    ExecutorInfo getExecutorInfoForSlave(SlaveID slave);

    class NMTaskFactoryImpl implements TaskFactory {
        public static final String EXECUTOR_NAME = "myriad_task";
        public static final String EXECUTOR_PREFIX = "myriad_executor";
        public static final String YARN_NODEMANAGER_OPTS_KEY = "YARN_NODEMANAGER_OPTS";
        private static final String YARN_RESOURCEMANAGER_HOSTNAME = "yarn.resourcemanager.hostname";

        private static final Logger LOGGER = LoggerFactory
                .getLogger(NMTaskFactoryImpl.class);
        private MyriadConfiguration cfg;
        private TaskUtils taskUtils;

        @Inject
        public NMTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils) {
            this.cfg = cfg;
            this.taskUtils = taskUtils;
        }

        private static String getFileName(String uri) {
            int lastSlash = uri.lastIndexOf('/');
            if (lastSlash == -1) {
                return uri;
            } else {
                String fileName = uri.substring(lastSlash + 1);
                Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName),
                        "URI should not have a slash at the end");
                return fileName;
            }
        }

        @Override
        public TaskInfo createTask(Offer offer, TaskID taskId, NodeTask nodeTask) {
            Objects.requireNonNull(offer, "Offer should be non-null");
            Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

            NMProfile profile = nodeTask.getProfile();
            NMTaskConfig nmTaskConfig = new NMTaskConfig();
            nmTaskConfig.setAdvertisableCpus(profile.getCpus());
            nmTaskConfig.setAdvertisableMem(profile.getMemory());
            NodeManagerConfiguration nodeManagerConfiguration = this.cfg.getNodeManagerConfiguration();
            nmTaskConfig.setUser(nodeManagerConfiguration
                    .getUser().orNull());
            nmTaskConfig.setJvmOpts(nodeManagerConfiguration
                    .getJvmOpts().orNull());
            nmTaskConfig.setCgroups(nodeManagerConfiguration.getCgroups().or(Boolean.FALSE));
            nmTaskConfig.setYarnEnvironment(cfg.getYarnEnvironment());

            // if RM's hostname is passed in as a system property, pass it along
            // to Node Managers launched via Myriad
            String rmHostName = System.getProperty(YARN_RESOURCEMANAGER_HOSTNAME);
            if (rmHostName != null && !rmHostName.isEmpty()) {

                String nmOpts = nmTaskConfig.getYarnEnvironment().get(YARN_NODEMANAGER_OPTS_KEY);
                if (nmOpts == null) {
                    nmOpts = "";
                }
                nmOpts += " " + "-D" + YARN_RESOURCEMANAGER_HOSTNAME + "=" + rmHostName;
                nmTaskConfig.getYarnEnvironment().put(YARN_NODEMANAGER_OPTS_KEY, nmOpts);
                LOGGER.info(YARN_RESOURCEMANAGER_HOSTNAME + " is set to " + rmHostName +
                    " via YARN_RESOURCEMANAGER_OPTS. Passing it into YARN_NODEMANAGER_OPTS.");
            }
//            else {
                // TODO(Santosh): Handle this case. Couple of options:
                // 1. Lookup a hostname here and use it as "RM's hostname"
                // 2. Abort here.. RM cannot start unless a hostname is passed in as it requires it to pass to NMs.

            String taskConfigJSON = new Gson().toJson(nmTaskConfig);

            Scalar taskMemory = Value.Scalar.newBuilder()
                    .setValue(taskUtils.getTaskMemory(profile)).build();
            Scalar taskCpus = Value.Scalar.newBuilder()
                    .setValue(taskUtils.getTaskCpus(profile)).build();
            ExecutorInfo executorInfo = getExecutorInfoForSlave(offer.getSlaveId());

            TaskInfo.Builder taskBuilder = TaskInfo.newBuilder()
                    .setName("task-" + taskId.getValue()).setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId());

            // TODO (mohit): Configure ports for multi-tenancy
            ByteString data = ByteString.copyFrom(taskConfigJSON.getBytes());
            return taskBuilder
                    .addResources(
                            Resource.newBuilder().setName("cpus")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(taskCpus).build())
                    .addResources(
                            Resource.newBuilder().setName("mem")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(taskMemory).build())
                    .setExecutor(executorInfo).setData(data).build();
        }

        @Override
        public ExecutorInfo getExecutorInfoForSlave(SlaveID slave) {
            Scalar executorMemory = Scalar.newBuilder()
                    .setValue(taskUtils.getExecutorMemory()).build();
            Scalar executorCpus = Scalar.newBuilder()
                    .setValue(taskUtils.getExecutorCpus()).build();

            String executorPath = cfg.getMyriadExecutorConfiguration()
                    .getPath();
            URI executorURI = URI.newBuilder().setValue(executorPath)
                    .setExecutable(true).build();

            CommandInfo commandInfo = CommandInfo.newBuilder()
                    .addUris(executorURI).setUser("root")
                    .setValue("export CAPSULE_CACHE_DIR=`pwd`;echo $CAPSULE_CACHE_DIR; java -Dcapsule.log=verbose -jar " + getFileName(executorPath))
                    .build();

            ExecutorID executorId = ExecutorID.newBuilder()
                .setValue(EXECUTOR_PREFIX + slave.getValue())
                    .build();
            return ExecutorInfo
                    .newBuilder()
                    .setCommand(commandInfo)
                    .setName(EXECUTOR_NAME)
                    .addResources(
                            Resource.newBuilder().setName("cpus")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(executorCpus).build())
                    .addResources(
                            Resource.newBuilder().setName("mem")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(executorMemory).build())
                .setExecutorId(executorId).build();
        }
    }
}
