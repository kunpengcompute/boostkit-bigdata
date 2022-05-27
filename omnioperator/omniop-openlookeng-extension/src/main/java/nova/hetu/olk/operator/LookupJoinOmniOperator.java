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
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.EmptyLookupSource;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.JoinOperatorFactory;
import io.prestosql.operator.JoinStatisticsCounter;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.LookupOuterOperator;
import io.prestosql.operator.LookupSourceFactory;
import io.prestosql.operator.LookupSourceProvider;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.StaticLookupSourceProvider;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.join.OmniLookupJoinOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.operator.LookupJoinOperators.JoinType.INNER;
import static io.prestosql.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_FULL;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_INNER;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_LEFT;
import static nova.hetu.omniruntime.constants.JoinType.OMNI_JOIN_TYPE_RIGHT;

/**
 * The type Lookup join omni operator.
 *
 * @since 20210630
 */
public class LookupJoinOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final Runnable afterClose;

    private final LookupSourceFactory lookupSourceFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;

    private final OmniOperator omniOperator;

    private LookupSourceProvider lookupSourceProvider;

    private Iterator<Page> result;

    private Page outputPage;

    private boolean closed;

    private State state = State.NEEDS_INPUT;

    private enum State
    {
        /**
         * Needs input state.
         */
        NEEDS_INPUT,
        /**
         * Has output state.
         */
        HAS_OUTPUT,
        /**
         * Finishing state.
         */
        FINISHING,
        /**
         * Finished state.
         */
        FINISHED
    }

    /**
     * Instantiates a new Lookup join omni operator.
     *
     * @param operatorContext the operator context
     * @param probeTypes the probe types
     * @param joinType the join type
     * @param lookupSourceFactory the lookup source factory
     * @param afterClose the after close
     * @param omniOperator the omni operator
     */
    public LookupJoinOmniOperator(OperatorContext operatorContext, List<Type> probeTypes, JoinType joinType,
                                  LookupSourceFactory lookupSourceFactory, Runnable afterClose, OmniOperator omniOperator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(probeTypes, "probeTypes is null");
        requireNonNull(joinType, "joinType is null");

        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.omniOperator = omniOperator;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        state = State.FINISHING;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = state == State.FINISHED;

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT && lookupSourceProviderFuture.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }
        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
        result = new VecBatchToPageIterator(omniOperator.getOutput());

        // here we get nothing from the native join, we can just keep the state and go
        // on
        if (!result.hasNext()) {
            result = null;
            return;
        }
        state = State.HAS_OUTPUT;
    }

    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider
                    .withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    @Override
    public Page getOutput()
    {
        if (state == State.NEEDS_INPUT || state == State.FINISHED) {
            return null;
        }

        // TODO introduce explicit state (enum), like in HBO
        if (!tryFetchLookupSourceProvider()) {
            if (!(state == State.FINISHING)) {
                return null;
            }

            verify(state == State.FINISHING);
            // We are no longer interested in the build side (the
            // lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        // it has all output or the output is empty
        if (state == State.FINISHING && (result == null || !result.hasNext())) {
            result = null;
            state = State.FINISHED;
            return null;
        }

        outputPage = result.next();
        if (outputPage != null) {
            Page output = outputPage;
            outputPage = null;
            if (!result.hasNext()) {
                result = null;
                if (state == State.FINISHING) {
                    state = State.FINISHED;
                }
                else if (state == State.HAS_OUTPUT) {
                    state = State.NEEDS_INPUT;
                }
            }
            return output;
        }

        return null;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        omniOperator.close();

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will
            // happen in reverse order.
            closer.register(afterClose::run);

            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The type Lookup join omni operator factory.
     *
     * @since 20210630
     */
    public static class LookupJoinOmniOperatorFactory
            extends AbstractOmniOperatorFactory
            implements JoinOperatorFactory
    {
        private final JoinType joinType;

        private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;

        private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;

        private final OptionalInt totalOperatorsCount;

        private final OmniLookupJoinOperatorFactory omniLookupJoinOperatorFactory;

        private boolean closed;

        /**
         * Instantiates a new Lookup join omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param lookupSourceFactoryManager the lookup source factory manager
         * @param probeTypes the probe types
         * @param probeOutputChannels the probe output channels
         * @param probeOutputChannelTypes the probe output channel types
         * @param joinType the join type
         * @param totalOperatorsCount the total operators count
         * @param probeJoinChannel the probe join channel
         * @param probeHashChannel the probe hash channel
         * @param hashBuilderOmniOperatorFactory the hash builder omni operator factory
         */
        public LookupJoinOmniOperatorFactory(int operatorId, PlanNodeId planNodeId,
                                             JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager, List<Type> probeTypes,
                                             List<Integer> probeOutputChannels, List<Type> probeOutputChannelTypes, JoinType joinType,
                                             OptionalInt totalOperatorsCount, List<Integer> probeJoinChannel, OptionalInt probeHashChannel,
                                             HashBuilderOmniOperator.HashBuilderOmniOperatorFactory hashBuilderOmniOperatorFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
            this.joinType = requireNonNull(joinType, "joinType is null");

            this.joinBridgeManager = lookupSourceFactoryManager;
            joinBridgeManager.incrementProbeFactoryCount();

            List<Integer> buildOutputChannels = hashBuilderOmniOperatorFactory.getOutputChannels();
            List<Type> buildOutputTypes = lookupSourceFactoryManager.getBuildOutputTypes();

            if (joinType == INNER || joinType == PROBE_OUTER) {
                this.outerOperatorFactoryResult = Optional.empty();
            }
            else {
                this.outerOperatorFactoryResult = Optional.of(new OuterOperatorFactoryResult(
                        new LookupOuterOperator.LookupOuterOperatorFactory(operatorId, planNodeId,
                                probeOutputChannelTypes, buildOutputTypes, lookupSourceFactoryManager),
                        lookupSourceFactoryManager.getBuildExecutionStrategy()));
            }
            this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");

            requireNonNull(probeHashChannel, "probeHashChannel is null");

            DataType[] types = OperatorUtils.toDataTypes(probeTypes);
            DataType[] buildOutputDataTypes = OperatorUtils.toDataTypes(buildOutputTypes);
            this.omniLookupJoinOperatorFactory = new OmniLookupJoinOperatorFactory(types,
                    Ints.toArray(probeOutputChannels), Ints.toArray(probeJoinChannel),
                    Ints.toArray(buildOutputChannels), buildOutputDataTypes, getOmniJoinType(joinType),
                    hashBuilderOmniOperatorFactory.getOmniHashBuilderOperatorFactory());
        }

        private nova.hetu.omniruntime.constants.JoinType getOmniJoinType(JoinType joinType)
        {
            nova.hetu.omniruntime.constants.JoinType omniJoinType;
            switch (joinType) {
                case INNER:
                    omniJoinType = OMNI_JOIN_TYPE_INNER;
                    break;
                case PROBE_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_LEFT;
                    break;
                case LOOKUP_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_RIGHT;
                    break;
                case FULL_OUTER:
                    omniJoinType = OMNI_JOIN_TYPE_FULL;
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported join type : " + joinType);
            }
            return omniJoinType;
        }

        private LookupJoinOmniOperatorFactory(LookupJoinOmniOperatorFactory other)
        {
            requireNonNull(other, "other is null");
            checkArgument(!other.closed, "cannot duplicated closed OperatorFactory");

            operatorId = other.operatorId;
            planNodeId = other.planNodeId;
            sourceTypes = other.sourceTypes;
            joinType = other.joinType;
            joinBridgeManager = other.joinBridgeManager;
            outerOperatorFactoryResult = other.outerOperatorFactoryResult;
            totalOperatorsCount = other.totalOperatorsCount;

            closed = false;
            joinBridgeManager.incrementProbeFactoryCount();
            omniLookupJoinOperatorFactory = other.omniLookupJoinOperatorFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, LookupJoinOmniOperator.class);

            LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    LookupJoinOmniOperator.class.getSimpleName());

            lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

            joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
            OmniOperator omniOperator = omniLookupJoinOperatorFactory.createOperator(vecAllocator);
            return new LookupJoinOmniOperator(operatorContext, sourceTypes, joinType, lookupSourceFactory,
                    () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()), omniOperator);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed);
            closed = true;
            joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.probeOperatorFactoryClosed(lifespan);
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LookupJoinOmniOperatorFactory(this);
        }

        @Override
        public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory()
        {
            return outerOperatorFactoryResult;
        }
    }
}
