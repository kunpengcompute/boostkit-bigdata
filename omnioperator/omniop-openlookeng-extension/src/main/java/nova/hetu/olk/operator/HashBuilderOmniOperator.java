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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.EmptyLookupSource;
import io.prestosql.operator.HashCollisionsCounter;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupSource;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.join.OmniHashBuilderOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type Hash builder omni operator.
 *
 * @since 20210630
 */
@ThreadSafe
public class HashBuilderOmniOperator
        implements Operator
{
    /**
     * The type Hash builder omni operator factory.
     *
     * @since 20210630
     */
    public static class HashBuilderOmniOperatorFactory
            extends AbstractOmniOperatorFactory
    {
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;

        private final List<Integer> outputChannels;

        private final OptionalInt preComputedHashChannel;

        private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();

        private final OmniHashBuilderOperatorFactory omniHashBuilderOperatorFactory;

        private boolean closed;

        /**
         * Instantiates a new Hash builder omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param buildTypes the build types
         * @param outputChannels the output channels
         * @param hashChannels the hash channels
         * @param preComputedHashChannel the pre computed hash channel
         * @param filterFunction the filter function factory
         * @param sortChannel the sort channel
         * @param searchFunctions the search function factories
         * @param operatorCount the operator count
         */
        public HashBuilderOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                              JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, List<Type> buildTypes,
                                              List<Integer> outputChannels, List<Integer> hashChannels, OptionalInt preComputedHashChannel,
                                              Optional<String> filterFunction, Optional<Integer> sortChannel, List<String> searchFunctions,
                                              int operatorCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(filterFunction, "filterFunction is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctions, "searchFunctions is null");
            checkArgument(sortChannel.isPresent() != searchFunctions.isEmpty(),
                    "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager,
                    "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));

            DataType[] omniBuildTypes = OperatorUtils.toDataTypes(buildTypes);
            String[] omniSearchFunctions = searchFunctions.stream().toArray(String[]::new);
            this.omniHashBuilderOperatorFactory = new OmniHashBuilderOperatorFactory(omniBuildTypes,
                    Ints.toArray(hashChannels), filterFunction, sortChannel, omniSearchFunctions, operatorCount);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, HashBuilderOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    HashBuilderOmniOperator.class.getSimpleName());

            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager
                    .getJoinBridge(driverContext.getLifespan());
            int partitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
            verify(partitionIndex < lookupSourceFactory.partitions());

            OmniOperator omniOperator = omniHashBuilderOperatorFactory.createOperator(vecAllocator);
            return new HashBuilderOmniOperator(operatorContext, lookupSourceFactory, partitionIndex, omniOperator);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }

        /**
         * Gets output channels.
         *
         * @return the output channels
         */
        public List<Integer> getOutputChannels()
        {
            return outputChannels;
        }

        /**
         * Gets omni hash builder operator factory.
         *
         * @return the omni hash builder operator factory
         */
        public OmniHashBuilderOperatorFactory getOmniHashBuilderOperatorFactory()
        {
            return omniHashBuilderOperatorFactory;
        }

        private int getAndIncrementPartitionIndex(Lifespan lifespan)
        {
            return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
        }
    }

    /**
     * The enum State.
     */
    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private final OperatorContext operatorContext;

    private final PartitionedLookupSourceFactory lookupSourceFactory;

    private final ListenableFuture<?> lookupSourceFactoryDestroyed;

    private final int partitionIndex;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;

    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();

    private final OmniOperator omniOperator;

    /**
     * Instantiates a new Hash builder omni operator.
     *
     * @param operatorContext the operator context
     * @param lookupSourceFactory the lookup source factory
     * @param partitionIndex the partition index
     * @param omniOperator the omni operator
     */
    public HashBuilderOmniOperator(OperatorContext operatorContext, PartitionedLookupSourceFactory lookupSourceFactory,
                                   int partitionIndex, OmniOperator omniOperator)
    {
        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;

        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.omniOperator = omniOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(
                        () -> new IllegalStateException("Lookup source built, but disposal future not set"));

            case CLOSED:
                return NOT_BLOCKED;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT);
        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);

        operatorContext.recordOutput(page.getSizeInBytes(), positionCount);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        switch (state) {
            case CONSUMING_INPUT:
                finishInput();
                return;

            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        omniOperator.getOutput();
        lookupSourceNotNeeded = Optional
                .of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, new EmptyJoinHashSupplier()));

        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        close();
    }

    @Override
    public boolean isFinished()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and
        // must clean resource up unconditionally
        omniOperator.close();
        state = State.CLOSED;
    }

    private class EmptyJoinHashSupplier
            implements Supplier<LookupSource>
    {
        @Override
        public LookupSource get()
        {
            return new EmptyLookupSource();
        }
    }
}
