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

package nova.hetu.olk.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.PartitionedOutputOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import nova.hetu.olk.tool.BlockUtils;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.partitionedoutput.OmniPartitionedOutPutOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type Omni project operator.
 *
 * @since 20210630
 */
public class PartitionedOutputOmniOperator
        implements Operator, Cloneable
{
    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = partitionFunction.isFull();
        return blocked.isDone() ? NOT_BLOCKED : blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (page.getPositionCount() == 0) {
            BlockUtils.freePage(page);
            return;
        }
        page = pagePreprocessor.apply(page);
        partitionFunction.partitionPage(omniOperator.getVecAllocator(), page);

        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());

        // We use getSizeInBytes() here instead of getRetainedSizeInBytes() for an
        // approximation of
        // the amount of memory used by the pageBuilders, because calculating the
        // retained
        // size can be expensive especially for complex types.
        long partitionsSizeInBytes = partitionFunction.getSizeInBytes();

        // We also add partitionsInitialRetainedSize as an approximation of the object
        // overhead of the partitions.
        systemMemoryContext.setBytes(partitionsSizeInBytes + partitionsInitialRetainedSize);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        finished = true;
        partitionFunction.flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public void close()
    {
        omniOperator.close();
    }

    /**
     * get PartitionedOutputInfo
     *
     * @return PartitionedOutputInfo
     */
    public PartitionedOutputOperator.PartitionedOutputInfo getInfo()
    {
        return partitionFunction.getInfo();
    }

    /**
     * PartitionedOutputOmniFactory
     */
    public static class PartitionedOutputOmniFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;
        private final int[] bucketToPartition;
        private final List<Type> hashChannelTypes;
        private boolean isHashPrecomputed = true;

        public PartitionedOutputOmniFactory(PartitionFunction partitionFunction, List<Integer> partitionChannels,
                                            List<Optional<NullableValue>> partitionConstants, boolean replicatesAnyRow, OptionalInt nullChannel,
                                            OutputBuffer outputBuffer, DataSize maxMemory, int[] bucketToPartition, boolean isHashPrecomputed,
                                            List<Type> hashChannelTypes)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
            this.hashChannelTypes = requireNonNull(hashChannelTypes, "hashChannelTypes is null");
            this.isHashPrecomputed = requireNonNull(isHashPrecomputed);
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId,
                                                    List<Type> types, Function<Page, Page> pagePreprocessor, TaskContext taskContext)
        {
            DataType[] sourceTypes = OperatorUtils.toDataTypes(types);
            int[] partitionChannelsArr = new int[partitionChannels.size()];
            for (int i = 0; i < partitionChannelsArr.length; i++) {
                partitionChannelsArr[i] = partitionChannels.get(i);
            }

            DataType[] hashChannelTpyesArr = OperatorUtils.toDataTypes(hashChannelTypes);
            int[] hashChannels = new int[hashChannelTypes.size()];
            for (int i = 0; i < hashChannelTypes.size(); i++) {
                hashChannels[i] = i;
            }

            OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory = new OmniPartitionedOutPutOperatorFactory(
                    sourceTypes, replicatesAnyRow, nullChannel, partitionChannelsArr,
                    partitionFunction.getPartitionCount(), bucketToPartition, isHashPrecomputed, hashChannelTpyesArr,
                    hashChannels);
            return new PartitionedOutputOmniOperatorFactory(operatorId, planNodeId, types, pagePreprocessor,
                    partitionFunction, partitionChannels, partitionConstants, replicatesAnyRow, nullChannel,
                    outputBuffer, maxMemory, omniPartitionedOutPutOperatorFactory);
        }
    }

    private static class PartitionedOutputOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final DataSize maxMemory;
        private final OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory;

        public PartitionedOutputOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes,
                                                    Function<Page, Page> pagePreprocessor, PartitionFunction partitionFunction,
                                                    List<Integer> partitionChannels, List<Optional<NullableValue>> partitionConstants,
                                                    boolean replicatesAnyRow, OptionalInt nullChannel, OutputBuffer outputBuffer, DataSize maxMemory,
                                                    OmniPartitionedOutPutOperatorFactory omniPartitionedOutPutOperatorFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            checkDataTypes(this.sourceTypes);
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.omniPartitionedOutPutOperatorFactory = omniPartitionedOutPutOperatorFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, PartitionedOutputOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    PartitionedOutputOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniPartitionedOutPutOperatorFactory.createOperator(vecAllocator);
            String id = operatorContext.getUniqueId();
            outputBuffer.addInputChannel(id);
            return new PartitionedOutputOmniOperator(id, operatorContext, sourceTypes, pagePreprocessor,
                    partitionFunction, partitionChannels, partitionConstants, replicatesAnyRow, nullChannel,
                    outputBuffer, maxMemory, omniOperator);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedOutputOmniOperatorFactory(operatorId, planNodeId, sourceTypes, pagePreprocessor,
                    partitionFunction, partitionChannels, partitionConstants, replicatesAnyRow, nullChannel,
                    outputBuffer, maxMemory, omniPartitionedOutPutOperatorFactory);
        }

        @Override
        public void checkDataType(Type type)
        {
            TypeSignature signature = type.getTypeSignature();
            String base = signature.getBase();

            switch (base) {
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                case StandardTypes.DOUBLE:
                case StandardTypes.BOOLEAN:
                case StandardTypes.VARBINARY:
                case StandardTypes.VARCHAR:
                case StandardTypes.CHAR:
                case StandardTypes.DECIMAL:
                case StandardTypes.DATE:
                case StandardTypes.ROW:
                    return;
                default:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Not support data Type " + base);
            }
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner partitionFunction;
    private final LocalMemoryContext systemMemoryContext;
    private final long partitionsInitialRetainedSize;
    private boolean finished;
    private final OmniOperator omniOperator;

    public PartitionedOutputOmniOperator(String id, OperatorContext operatorContext, List<Type> sourceTypes,
                                         Function<Page, Page> pagePreprocessor, PartitionFunction partitionFunction, List<Integer> partitionChannels,
                                         List<Optional<NullableValue>> partitionConstants, boolean replicatesAnyRow, OptionalInt nullChannel,
                                         OutputBuffer outputBuffer, DataSize maxMemory, OmniOperator omniOperator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.omniOperator = omniOperator;

        this.partitionFunction = new PagePartitioner(id, operatorContext, partitionFunction, partitionChannels,
                partitionConstants, replicatesAnyRow, nullChannel, outputBuffer,
                operatorContext.getDriverContext().getSerde(), sourceTypes, maxMemory, omniOperator);

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext
                .newLocalSystemMemoryContext(PartitionedOutputOmniOperator.class.getSimpleName());
        this.partitionsInitialRetainedSize = this.partitionFunction.getRetainedSizeInBytes();
        this.systemMemoryContext.setBytes(partitionsInitialRetainedSize);
    }

    private static class PagePartitioner
    {
        private static final Logger LOG = Logger.get(PartitionedOutputOmniOperator.class);
        private final String id;
        private final OmniOperator omniOperator;
        private final OutputBuffer outputBuffer;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final PagesSerde serde;
        private final PageBuilder[] pageBuilders;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();

        private OperatorContext operatorContext;

        public PagePartitioner(String id, OperatorContext operatorContext, PartitionFunction partitionFunction,
                               List<Integer> partitionChannels, List<Optional<NullableValue>> partitionConstants,
                               boolean replicatesAnyRow, OptionalInt nullChannel, OutputBuffer outputBuffer, PagesSerde serde,
                               List<Type> sourceTypes, DataSize maxMemory, OmniOperator omniOperator)
        {
            this.omniOperator = omniOperator;
            this.id = id;
            this.operatorContext = operatorContext;
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(NullableValue::asBlock)).collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.serde = requireNonNull(serde, "serdeFactory is null");

            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);
            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }
        }

        /**
         * outputBuffer is full ?
         *
         * @return ListenableFuture
         */
        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        /**
         * get pagebuilder size
         *
         * @return pagebuilder bytes size
         */
        public long getSizeInBytes()
        {
            // We use a foreach loop instead of streams
            // as it has much better performance.
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getSizeInBytes();
            }
            return sizeInBytes;
        }

        /**
         * This method can be expensive for complex types.
         *
         * @return bytes
         */
        public long getRetainedSizeInBytes()
        {
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getRetainedSizeInBytes();
            }
            return sizeInBytes;
        }

        /**
         * get PartitionedOutputInfo
         *
         * @return PartitionedOutputInfo
         */
        public PartitionedOutputOperator.PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputOperator.PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(),
                    outputBuffer.getPeakMemoryUsage());
        }

        /**
         * partition Page
         *
         * @param vecAllocator vector allocator
         * @param page page
         */
        public void partitionPage(VecAllocator vecAllocator, Page page)
        {
            requireNonNull(page, "page is null");

            VecBatch originalVecBatch = buildVecBatch(vecAllocator, page, this);
            VecBatch originalAndPartitionArgVecBatch = addPartitionFunctionArguments(originalVecBatch);

            omniOperator.addInput(originalAndPartitionArgVecBatch);
            flush(true);

            originalVecBatch.close();
        }

        private VecBatch addPartitionFunctionArguments(VecBatch vecBatch)
        {
            int positionCount = vecBatch.getRowCount();
            ArrayList<Vec> vecList = new ArrayList<>(Arrays.asList(vecBatch.getVectors()));
            for (int i = 0; i < partitionChannels.size(); i++) {
                Optional<Block> partitionConstant = partitionConstants.get(i);
                if (partitionConstant.isPresent()) {
                    Block block = OperatorUtils.buildOffHeapBlock(omniOperator.getVecAllocator(),
                            partitionConstant.get());
                    // Because there is no vec corresponding to RunLengthEncodedBlock,
                    // the original data is directly constructed.
                    int[] positions = new int[positionCount];
                    Arrays.fill(positions, 0);
                    vecList.add(((Vec) block.getValues()).copyPositions(positions, 0, positionCount));
                    // there is no need to release partitionConstant because it is converted from
                    // NullableValue, which is always in on heap
                    block.close();
                }
                else {
                    vecList.add(vecList.get(partitionChannels.get(i)).slice(0, positionCount));
                }
            }
            return new VecBatch(vecList, positionCount);
        }

        /**
         * write data to pages
         *
         * @param force force write
         */
        public void flush(boolean force)
        {
            // add all full pages to output buffer
            VecBatchToPageIterator pageIterator = new VecBatchToPageIterator(omniOperator.getOutput());
            int partition = 0;
            if (force) {
                // if (!partitionPageBuilder.isEmpty() && (force ||
                // partitionPageBuilder.isFull()))
                while (pageIterator.hasNext()) {
                    Page pagePartition = pageIterator.next();
                    Page onHeapPage = OperatorUtils.transferToOnHeapPage(pagePartition);
                    SerializedPage serializedPage = serde.serialize(onHeapPage);
                    BlockUtils.freePage(pagePartition);
                    outputBuffer.enqueue(partition, ImmutableList.of(serializedPage), id);
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                    partition++;
                }
            }
        }
    }
}
