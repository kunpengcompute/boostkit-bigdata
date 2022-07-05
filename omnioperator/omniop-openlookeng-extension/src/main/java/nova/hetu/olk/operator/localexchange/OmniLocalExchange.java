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

package nova.hetu.olk.operator.localexchange;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.exchange.BroadcastExchanger;
import io.prestosql.operator.exchange.LocalExchange;
import io.prestosql.operator.exchange.LocalExchangeSource;
import io.prestosql.operator.exchange.PageReference;
import io.prestosql.operator.exchange.PassthroughExchanger;
import io.prestosql.operator.exchange.RandomExchanger;
import io.prestosql.operator.exchange.SortBasedAggregationPartitioningExchanger;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;

@ThreadSafe
public class OmniLocalExchange
        extends LocalExchange
{
    public OmniLocalExchange(int sinkFactoryCount, int bufferCount, PartitioningHandle partitioning,
                             List<? extends Type> types, List<Integer> partitionChannels, Optional<Integer> partitionHashChannel,
                             DataSize maxBufferedBytes, boolean isForMerge, TaskContext taskContext, String id, boolean snapshotEnabled,
                             AggregationNode.AggregationType aggregationType)
    {
        super(sinkFactoryCount, bufferCount, partitioning, types, partitionChannels, partitionHashChannel,
                maxBufferedBytes, isForMerge, taskContext, id, snapshotEnabled, aggregationType);

        ImmutableList.Builder<LocalExchangeSource> localExchangeSourceBuilder = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            // Snapshot state is given to all local-sources, so they can process markers when they are received.
            localExchangeSourceBuilder.add(new OmniLocalExchangeSource(source -> checkAllSourcesFinished(), snapshotState));
        }
        this.sources = localExchangeSourceBuilder.build();

        List<BiConsumer<PageReference, String>> buffers = this.sources.stream()
                .map(buffer -> (BiConsumer<PageReference, String>) buffer::addPage).collect(toImmutableList());

        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            if (!aggregationType.equals(AggregationNode.AggregationType.SORT_BASED)) {
                exchangerSupplier = () -> new OmniPartitioningExchanger(buffers, this.memoryManager, types,
                        partitionChannels, partitionHashChannel, taskContext);
            }
            else {
                exchangerSupplier = () -> new SortBasedAggregationPartitioningExchanger(buffers, memoryManager, types,
                        partitionChannels, partitionHashChannel, taskContext.getSession());
            }
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            Iterator<LocalExchangeSource> sourceIterator = this.sources.iterator();
            exchangerSupplier = () -> {
                checkState(sourceIterator.hasNext(), "no more sources");
                return new PassthroughExchanger(sourceIterator.next(), maxBufferedBytes.toBytes() / bufferCount, memoryManager::updateMemoryUsage, isForMerge);
            };
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    @ThreadSafe
    public static class OmniLocalExchangeFactory
            extends LocalExchangeFactory
    {
        public OmniLocalExchangeFactory(PartitioningHandle partitioning, int defaultConcurrency, List<Type> types,
                                        List<Integer> partitionChannels, Optional<Integer> partitionHashChannel,
                                        PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy, DataSize maxBufferedBytes)
        {
            super(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel,
                    exchangeSourcePipelineExecutionStrategy, maxBufferedBytes);
        }

        public OmniLocalExchangeFactory(PartitioningHandle partitioning, int defaultConcurrency, List<Type> types,
                                        List<Integer> partitionChannels, Optional<Integer> partitionHashChannel,
                                        PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy, DataSize maxBufferedBytes,
                                        boolean isForMerge, AggregationNode.AggregationType aggregationType)
        {
            super(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel,
                    exchangeSourcePipelineExecutionStrategy, maxBufferedBytes, isForMerge, aggregationType);
        }

        @Override
        public LocalExchange getLocalExchange(Lifespan lifespan)
        {
            return getLocalExchange(lifespan, null, null, false);
        }

        @Override
        public LocalExchange getLocalExchange(Lifespan lifespan, TaskContext taskContext)
        {
            return getLocalExchange(lifespan, taskContext, null, false);
        }

        @Override
        public synchronized LocalExchange getLocalExchange(Lifespan lifespan, TaskContext taskContext, String id,
                                                           boolean snapshotEnabled)
        {
            if (exchangeSourcePipelineExecutionStrategy == UNGROUPED_EXECUTION) {
                checkArgument(lifespan.isTaskWide(),
                        "OmniLocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");
            }
            else {
                checkArgument(!lifespan.isTaskWide(),
                        "OmniLocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
            }
            return localExchangeMap.computeIfAbsent(lifespan, ignored -> {
                checkState(noMoreSinkFactories);
                LocalExchange localExchange = new OmniLocalExchange(numSinkFactories, bufferCount, partitioning, types,
                        partitionChannels, partitionHashChannel, maxBufferedBytes, isForMerge, taskContext, null, false,
                        aggregationType);
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
        }
    }
}
