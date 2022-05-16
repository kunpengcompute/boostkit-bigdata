/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package nova.hetu.olk.tool;

import io.airlift.log.Logger;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskState;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.block.Block;
import nova.hetu.olk.memory.OpenLooKengAllocatorFactory;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

public class VecAllocatorHelper
{
    private static final Logger log = Logger.get(VecAllocatorHelper.class);

    private static final String VECTOR_ALLOCATOR_PROPERTY_NAME = "vector_allocator";

    public static final long DEFAULT_RESERVATION = 1 << 20; // 1MB

    private VecAllocatorHelper()
    {
    }

    public static void setVectorAllocatorToTaskContext(TaskContext taskContext, VecAllocator vecAllocator)
    {
        taskContext.getTaskExtendProperties().put(VECTOR_ALLOCATOR_PROPERTY_NAME, vecAllocator);
    }

    private static VecAllocator getVecAllocatorFromTaskContext(TaskContext taskContext)
    {
        Object obj = taskContext.getTaskExtendProperties().get(VECTOR_ALLOCATOR_PROPERTY_NAME);
        if (obj instanceof VecAllocator) {
            return (VecAllocator) obj;
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }

    public static VecAllocator getVecAllocatorFromBlocks(Block[] blocks)
    {
        for (Block block : blocks) {
            if (block.isExtensionBlock()) {
                return ((Vec) block.getValues()).getAllocator();
            }
        }
        return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
    }

    /**
     * create an operator level allocator based on driver context.
     *
     * @param driverContext diver context
     * @param limit allocator limit
     * @param jazz operator Class
     * @return operator allocator
     */
    public static VecAllocator createOperatorLevelAllocator(DriverContext driverContext, long limit, Class<?> jazz)
    {
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        VecAllocator vecAllocator = getVecAllocatorFromTaskContext(taskContext);
        return createOperatorLevelAllocator(vecAllocator, limit, taskContext.getTaskId().toString(), 0, jazz);
    }

    /**
     * create an operator level allocator based on driver context.
     *
     * @param driverContext diver context
     * @param limit allocator limit
     * @param reservation reservation
     * @param jazz operator Class
     * @return operator allocator
     */
    public static VecAllocator createOperatorLevelAllocator(DriverContext driverContext, long limit, long reservation,
                                                            Class<?> jazz)
    {
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        VecAllocator vecAllocator = getVecAllocatorFromTaskContext(taskContext);
        return createOperatorLevelAllocator(vecAllocator, limit, taskContext.getTaskId().toString(), reservation, jazz);
    }

    /**
     * create an operator level allocator base on a vecAllocator.
     *
     * @param parent parent vecAllocator
     * @param limit allocator limit
     * @param prefix taskId
     * @param reservation allocator default reservation
     * @param jazz operator Class
     * @return operator allocator
     */
    private static VecAllocator createOperatorLevelAllocator(VecAllocator parent, long limit, String prefix,
                                                             long reservation, Class<?> jazz)
    {
        if (parent == VecAllocator.GLOBAL_VECTOR_ALLOCATOR || parent == null) {
            return VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
        }
        return parent.newChildAllocator(prefix + jazz.getSimpleName(), limit, reservation);
    }

    /**
     * create an operator level allocator based on task context.
     *
     * @param taskContext task context
     * @param limit allocator limit
     * @param jazz operator Class
     * @return operator allocator
     */
    public static VecAllocator createOperatorLevelAllocator(TaskContext taskContext, long limit, long reservation,
                                                            Class<?> jazz)
    {
        VecAllocator vecAllocator = getVecAllocatorFromTaskContext(taskContext);
        return createOperatorLevelAllocator(vecAllocator, limit, taskContext.getTaskId().toString(), 0, jazz);
    }

    /**
     * create task level allocator
     *
     * @param taskContext task context
     * @return task vec allocator
     */
    public static VecAllocator createTaskLevelAllocator(TaskContext taskContext)
    {
        TaskId taskId = taskContext.getTaskId();
        VecAllocator vecAllocator = OpenLooKengAllocatorFactory.create(taskId.toString(), () -> {
            taskContext.getTaskStateMachine().addStateChangeListenerToTail(state -> {
                if (state.isDone()) {
                    if (state == TaskState.FINISHED) {
                        OpenLooKengAllocatorFactory.delete(taskId.toString());
                    }
                    else {
                        // CANCELED, ABORTED, FAILED and so on, wait for the completion of all drivers fo the task,
                        // here the allocator will be released when the gc recycles
                        VecAllocator removedAllocator = OpenLooKengAllocatorFactory.remove(taskId.toString());
                        if (removedAllocator != null) {
                            log.debug("remove allocator from cache:" + removedAllocator.getScope());
                        }
                    }
                }
            });
        });
        VecAllocatorHelper.setVectorAllocatorToTaskContext(taskContext, vecAllocator);
        return vecAllocator;
    }
}
