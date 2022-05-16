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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.WindowFunctionDefinition;
import io.prestosql.operator.window.FrameInfo;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.olk.tool.VecBatchToPageIterator;
import nova.hetu.omniruntime.constants.FunctionType;
import nova.hetu.omniruntime.constants.OmniWindowFrameBoundType;
import nova.hetu.omniruntime.constants.OmniWindowFrameType;
import nova.hetu.omniruntime.operator.OmniOperator;
import nova.hetu.omniruntime.operator.window.OmniWindowOperatorFactory;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecBatch;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static nova.hetu.olk.tool.OperatorUtils.buildVecBatch;

/**
 * The type Window omni operator.
 *
 * @since 20210630
 */
public class WindowOmniOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final OmniOperator omniOperator;

    /**
     * The Pages.
     */
    Iterator<Page> pages;

    private boolean finishing;

    private boolean finished;

    /**
     * Instantiates a new Window omni operator.
     *
     * @param operatorContext the operator context
     * @param omniOperator the omni operator
     */
    public WindowOmniOperator(OperatorContext operatorContext, OmniOperator omniOperator)
    {
        this.operatorContext = operatorContext;
        this.omniOperator = omniOperator;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close() throws Exception
    {
        omniOperator.close();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        VecBatch vecBatch = buildVecBatch(omniOperator.getVecAllocator(), page, this);
        omniOperator.addInput(vecBatch);
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        if (finishing) {
            if (pages == null) {
                pages = new VecBatchToPageIterator(omniOperator.getOutput());
            }
            else {
                if (pages.hasNext()) {
                    return pages.next();
                }
                else {
                    finished = true;
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return Operator.super.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke()
    {
        Operator.super.finishMemoryRevoke();
    }

    /**
     * The type Window omni operator factory.
     *
     * @since 20210630
     */
    public static class WindowOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;

        private final PlanNodeId planNodeId;

        private final List<Type> sourceTypes;

        private final ImmutableList<Integer> outputChannels;

        private final ImmutableList<WindowFunctionDefinition> windowFunctionDefinitions;

        private final ImmutableList<Integer> partitionChannels;

        private final ImmutableList<Integer> preGroupedChannels;

        private final ImmutableList<Integer> sortChannels;

        private final ImmutableList<SortOrder> sortOrder;

        private final int preSortedChannelPrefix;

        private final int expectedPositions;

        private OmniWindowOperatorFactory omniWindowOperatorFactory;

        /**
         * Instantiates a new Window omni operator factory.
         *
         * @param operatorId the operator id
         * @param planNodeId the plan node id
         * @param sourceTypes the source types
         * @param outputChannels the output channels
         * @param windowFunctionDefinitions the window function definitions
         * @param partitionChannels the partition channels
         * @param preGroupedChannels the pre grouped channels
         * @param sortChannels the sort channels
         * @param sortOrder the sort order
         * @param preSortedChannelPrefix the pre sorted channel prefix
         * @param expectedPositions the expected positions
         */
        public WindowOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<? extends Type> sourceTypes,
                                         List<Integer> outputChannels, List<WindowFunctionDefinition> windowFunctionDefinitions,
                                         List<Integer> partitionChannels, List<Integer> preGroupedChannels, List<Integer> sortChannels,
                                         List<SortOrder> sortOrder, int preSortedChannelPrefix, int expectedPositions)
        {
            requireNonNull(sourceTypes, "sourceTypes is null");
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(outputChannels, "outputChannels is null");
            requireNonNull(windowFunctionDefinitions, "windowFunctionDefinitions is null");
            requireNonNull(partitionChannels, "partitionChannels is null");
            requireNonNull(preGroupedChannels, "preGroupedChannels is null");
            checkArgument(partitionChannels.containsAll(preGroupedChannels),
                    "preGroupedChannels must be a subset of partitionChannels");
            requireNonNull(sortChannels, "sortChannels is null");
            requireNonNull(sortOrder, "sortOrder is null");
            checkArgument(sortChannels.size() == sortOrder.size(),
                    "Must have same number of sort channels as sort orders");
            checkArgument(preSortedChannelPrefix <= sortChannels.size(),
                    "Cannot have more pre-sorted channels than specified sorted channels");
            checkArgument(
                    preSortedChannelPrefix == 0
                            || ImmutableSet.copyOf(preGroupedChannels).equals(ImmutableSet.copyOf(partitionChannels)),
                    "preSortedChannelPrefix can only be greater than zero if all partition channels are pre-grouped");

            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = ImmutableList.copyOf(outputChannels);
            this.windowFunctionDefinitions = ImmutableList.copyOf(windowFunctionDefinitions);
            this.partitionChannels = ImmutableList.copyOf(partitionChannels);
            this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrder = ImmutableList.copyOf(sortOrder);
            this.preSortedChannelPrefix = preSortedChannelPrefix;
            this.expectedPositions = expectedPositions;

            omniWindowOperatorFactory = getOmniWindowOperatorFactory(sourceTypes, outputChannels,
                    windowFunctionDefinitions, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                    preSortedChannelPrefix, expectedPositions);
        }

        private OmniWindowOperatorFactory getOmniWindowOperatorFactory(List<? extends Type> sourceTypes,
                                                                       List<Integer> outputChannels, List<WindowFunctionDefinition> windowFunctionDefinitions,
                                                                       List<Integer> partitionChannels, List<Integer> preGroupedChannels, List<Integer> sortChannels,
                                                                       List<SortOrder> sortOrder, int preSortedChannelPrefix, int expectedPositions)
        {
            DataType[] omniSourceTypes = OperatorUtils.toDataTypes(sourceTypes);
            int[] omniOutputChannels = outputChannels.stream().mapToInt(Integer::valueOf).toArray();
            FunctionType[] windowFunctionType = getWindowFunctionTypes(windowFunctionDefinitions);
            int[] omniPartitionChannels = partitionChannels.stream().mapToInt(Integer::valueOf).toArray();
            int[] omnipreGroupedChannels = preGroupedChannels.stream().mapToInt(Integer::valueOf).toArray();
            int[] omniSortChannels = sortChannels.stream().mapToInt(Integer::valueOf).toArray();
            int[] omniSortOrder = new int[sortOrder.size()];
            int[] omniSortNullFirst = new int[sortOrder.size()];
            for (int i = 0; i < sortOrder.size(); i++) {
                if (sortOrder.get(i).isAscending()) {
                    omniSortOrder[i] = 1;
                }
                else {
                    omniSortOrder[i] = 0;
                }
                if (sortOrder.get(i).isNullsFirst()) {
                    omniSortNullFirst[i] = 1;
                }
                else {
                    omniSortNullFirst[i] = 0;
                }
            }
            int[] argumentChannels = getArgumentChannels(windowFunctionDefinitions);
            DataType[] omniWindowReturnTypes = getDataTypes(windowFunctionDefinitions);

            OmniWindowFrameType[] frameTypes = getFrameTypes(windowFunctionDefinitions);
            OmniWindowFrameBoundType[] frameStartTypes = getFrameStartType(windowFunctionDefinitions);
            int[] frameStartChannels = windowFunctionDefinitions.stream().map(WindowFunctionDefinition::getFrameInfo)
                    .mapToInt(FrameInfo::getStartChannel).toArray();
            OmniWindowFrameBoundType[] frameEndTypes = getFrameEndType(windowFunctionDefinitions);
            int[] frameEndChannels = windowFunctionDefinitions.stream().map(WindowFunctionDefinition::getFrameInfo)
                    .mapToInt(FrameInfo::getEndChannel).toArray();
            OmniWindowOperatorFactory operatorFactory = new OmniWindowOperatorFactory(omniSourceTypes,
                    omniOutputChannels, windowFunctionType, omniPartitionChannels, omnipreGroupedChannels,
                    omniSortChannels, omniSortOrder, omniSortNullFirst, preSortedChannelPrefix, expectedPositions,
                    argumentChannels, omniWindowReturnTypes, frameTypes, frameStartTypes, frameStartChannels,
                    frameEndTypes, frameEndChannels);
            this.omniWindowOperatorFactory = operatorFactory;
            return omniWindowOperatorFactory;
        }

        // get frame type
        private OmniWindowFrameType[] getFrameTypes(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            OmniWindowFrameType[] omniWindowFrameTypes = new OmniWindowFrameType[windowFunctionDefinitions.size()];

            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                omniWindowFrameTypes[i] = OperatorUtils
                        .toWindowFrameType(windowFunctionDefinitions.get(i).getFrameInfo().getType());
            }
            return omniWindowFrameTypes;
        }

        // get frame start type
        private OmniWindowFrameBoundType[] getFrameStartType(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            Types.FrameBoundType[] startTypes = new Types.FrameBoundType[windowFunctionDefinitions.size()];
            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                startTypes[i] = windowFunctionDefinitions.get(i).getFrameInfo().getStartType();
            }
            return toFrameBoundType(startTypes);
        }

        // get frame end type
        private OmniWindowFrameBoundType[] getFrameEndType(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            Types.FrameBoundType[] endTypes = new Types.FrameBoundType[windowFunctionDefinitions.size()];
            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                endTypes[i] = windowFunctionDefinitions.get(i).getFrameInfo().getEndType();
            }
            return toFrameBoundType(endTypes);
        }

        private OmniWindowFrameBoundType[] toFrameBoundType(Types.FrameBoundType[] boundTypes)
        {
            OmniWindowFrameBoundType[] omniWindowBoundTypes = new OmniWindowFrameBoundType[boundTypes.length];

            for (int i = 0; i < boundTypes.length; i++) {
                omniWindowBoundTypes[i] = OperatorUtils.toWindowFrameBoundType(boundTypes[i]);
            }
            return omniWindowBoundTypes;
        }

        private DataType[] getDataTypes(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            DataType[] omniWindowReturnTypes = new DataType[windowFunctionDefinitions.size()];

            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                omniWindowReturnTypes[i] = OperatorUtils.toDataType(windowFunctionDefinitions.get(i).getType());
            }
            return omniWindowReturnTypes;
        }

        private int[] getArgumentChannels(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            int[] argumentChannels = new int[windowFunctionDefinitions.size()];
            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                if (windowFunctionDefinitions.get(i).getArgumentChannels().size() == 0) {
                    argumentChannels[i] = -1;
                }
                else if (windowFunctionDefinitions.get(i).getArgumentChannels().size() == 1) {
                    argumentChannels[i] = windowFunctionDefinitions.get(i).getArgumentChannels().get(0);
                }
                else {
                    throw new UnsupportedOperationException(
                            "Unsupported! WindowFunctionDefinition.getArgumentChannels() bigger than 1: "
                                    + windowFunctionDefinitions.get(i).getArgumentChannels().size());
                }
            }
            return argumentChannels;
        }

        private FunctionType[] getWindowFunctionTypes(List<WindowFunctionDefinition> windowFunctionDefinitions)
        {
            FunctionType[] windowFunctionType = new FunctionType[windowFunctionDefinitions.size()];
            for (int i = 0; i < windowFunctionDefinitions.size(); i++) {
                switch (windowFunctionDefinitions.get(i).getFunctionSupplier().getSignature().getName()
                        .getObjectName()) {
                    case "rank":
                        windowFunctionType[i] = FunctionType.OMNI_WINDOW_TYPE_RANK;
                        break;
                    case "row_number":
                        windowFunctionType[i] = FunctionType.OMNI_WINDOW_TYPE_ROW_NUMBER;
                        break;
                    case "avg":
                        windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_AVG;
                        break;
                    case "sum":
                        windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_SUM;
                        break;
                    case "count": {
                        if (windowFunctionDefinitions.get(i).getArgumentChannels().size() == 0) {
                            windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_COUNT_ALL;
                        }
                        else {
                            windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_COUNT_COLUMN;
                        }
                        break;
                    }
                    case "max":
                        windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_MAX;
                        break;
                    case "min":
                        windowFunctionType[i] = FunctionType.OMNI_AGGREGATION_TYPE_MIN;
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported Aggregator type by OmniRuntime: "
                                + windowFunctionDefinitions.get(i).getFunctionSupplier().getSignature().getName());
                }
            }
            return windowFunctionType;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, WindowOmniOperator.class);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    WindowOmniOperator.class.getSimpleName());
            OmniOperator omniOperator = omniWindowOperatorFactory.createOperator(vecAllocator);
            return new WindowOmniOperator(operatorContext, omniOperator);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new WindowOmniOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels,
                    windowFunctionDefinitions, partitionChannels, preGroupedChannels, sortChannels, sortOrder,
                    preSortedChannelPrefix, expectedPositions);
        }

        @Override
        public boolean isExtensionOperatorFactory()
        {
            return true;
        }

        @Override
        public List<Type> getSourceTypes()
        {
            return sourceTypes;
        }
    }
}
