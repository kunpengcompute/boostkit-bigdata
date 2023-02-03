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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Split;
import io.prestosql.operator.BloomFilterUtils;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.SourceOperator;
import io.prestosql.operator.SourceOperatorFactory;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.operator.WorkProcessorSourceOperator;
import io.prestosql.operator.WorkProcessorSourceOperatorAdapter;
import io.prestosql.operator.WorkProcessorSourceOperatorFactory;
import io.prestosql.operator.project.CursorProcessor;
import io.prestosql.operator.project.CursorProcessorOutput;
import io.prestosql.operator.project.MergePages;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.EmptySplitPageSource;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.statestore.StateStoreProvider;
import nova.hetu.olk.OmniLocalExecutionPlanner;
import nova.hetu.olk.OmniLocalExecutionPlanner.OmniLocalExecutionPlanContext;
import nova.hetu.olk.operator.filterandproject.OmniMergePages;
import nova.hetu.olk.operator.filterandproject.OmniPageProcessor;
import nova.hetu.olk.tool.VecAllocatorHelper;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static nova.hetu.olk.tool.OperatorUtils.transferToOffHeapPages;

public class ScanFilterAndProjectOmniOperator
        implements WorkProcessorSourceOperator
{
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;

    private final WorkProcessor<Page> pages;

    private RecordCursor cursor;
    private ConnectorPageSource pageSource;

    private long processedPositions;
    private long processedBytes;
    private long physicalBytes;
    private long readTimeNanos;
    private VecAllocator vecAllocator;
    private List<Type> inputTypes;
    private OmniMergePages.OmniMergePagesTransformation omniMergePagesTransformation;

    private PageProcessor pageProcessor;

    private static final Logger log = Logger.get(ScanFilterAndProjectOmniOperator.class);

    public ScanFilterAndProjectOmniOperator(Session session, MemoryTrackingContext memoryTrackingContext,
                                            DriverYieldSignal yieldSignal, WorkProcessor<Split> splits, PageSourceProvider pageSourceProvider,
                                            CursorProcessor cursorProcessor, PageProcessor pageProcessor, TableHandle table,
                                            Iterable<ColumnHandle> columns, Optional<DynamicFilterSupplier> dynamicFilter, Iterable<Type> types,
                                            DataSize minOutputPageSize, int minOutputPageRowCount, Optional<TableScanNode> tableScanNodeOptional,
                                            Optional<StateStoreProvider> stateStoreProviderOptional, Optional<QueryId> queryIdOptional,
                                            Optional<Metadata> metadataOptional, Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
                                            VecAllocator vecAllocator, List<Type> inputTypes, OmniLocalExecutionPlanContext context)
    {
        pages = splits.flatTransform(new SplitToPages(session, yieldSignal, pageSourceProvider, cursorProcessor,
                pageProcessor, table, columns, dynamicFilter, types,
                requireNonNull(memoryTrackingContext, "memoryTrackingContext is null").aggregateSystemMemoryContext(),
                minOutputPageSize, minOutputPageRowCount, tableScanNodeOptional, stateStoreProviderOptional,
                queryIdOptional, metadataOptional, dynamicFilterCacheManagerOptional, context));
        this.vecAllocator = vecAllocator;
        this.inputTypes = inputTypes;
        this.pageProcessor = requireNonNull(pageProcessor, "processor is null");
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
    {
        return () -> {
            if (pageSource instanceof UpdatablePageSource) {
                return Optional.of((UpdatablePageSource) pageSource);
            }
            return Optional.empty();
        };
    }

    @Override
    public DataSize getPhysicalInputDataSize()
    {
        return new DataSize(physicalBytes, BYTE);
    }

    @Override
    public long getPhysicalInputPositions()
    {
        return processedPositions;
    }

    @Override
    public DataSize getInputDataSize()
    {
        return new DataSize(processedBytes, BYTE);
    }

    @Override
    public long getInputPositions()
    {
        return processedPositions;
    }

    @Override
    public Duration getReadTime()
    {
        return new Duration(readTimeNanos, NANOSECONDS);
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
    {
        if (pageSource != null) {
            try {
                pageSource.close();
                if (omniMergePagesTransformation != null) {
                    omniMergePagesTransformation.close();
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else if (cursor != null) {
            cursor.close();
        }
        if (pageProcessor instanceof OmniPageProcessor) {
            ((OmniPageProcessor) pageProcessor).close();
        }
    }

    // Table scan operators do not participate in snapshotting
    @RestorableConfig(unsupported = true)
    private class SplitToPages
            implements WorkProcessor.Transformation<Split, WorkProcessor<Page>>
    {
        final Session session;
        final DriverYieldSignal yieldSignal;
        final PageSourceProvider pageSourceProvider;
        final CursorProcessor cursorProcessor;
        final PageProcessor pageProcessor;
        final TableHandle table;
        final List<ColumnHandle> columns;
        final Optional<DynamicFilterSupplier> dynamicFilter;
        final List<Type> types;
        final LocalMemoryContext memoryContext;
        final AggregatedMemoryContext localAggregatedMemoryContext;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;
        final DataSize minOutputPageSize;
        final Optional<TableScanNode> tableScanNodeOptional;
        final Optional<StateStoreProvider> stateStoreProviderOptional;
        final Optional<QueryId> queryIdOptional;
        final Optional<Metadata> metadataOptional;
        final Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
        final int minOutputPageRowCount;
        final OmniLocalExecutionPlanContext context;

        SplitToPages(Session session, DriverYieldSignal yieldSignal, PageSourceProvider pageSourceProvider,
                     CursorProcessor cursorProcessor, PageProcessor pageProcessor, TableHandle table,
                     Iterable<ColumnHandle> columns, Optional<DynamicFilterSupplier> dynamicFilter, Iterable<Type> types,
                     AggregatedMemoryContext aggregatedMemoryContext, DataSize minOutputPageSize, int minOutputPageRowCount,
                     Optional<TableScanNode> tableScanNodeOptional, Optional<StateStoreProvider> stateStoreProviderOptional,
                     Optional<QueryId> queryIdOptional, Optional<Metadata> metadataOptional,
                     Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
                     OmniLocalExecutionPlanContext context)
        {
            this.session = requireNonNull(session, "session is null");
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = dynamicFilter;
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.memoryContext = aggregatedMemoryContext
                    .newLocalMemoryContext(ScanFilterAndProjectOmniOperator.class.getSimpleName());
            this.localAggregatedMemoryContext = newSimpleAggregatedMemoryContext();
            this.pageSourceMemoryContext = localAggregatedMemoryContext
                    .newLocalMemoryContext(ScanFilterAndProjectOmniOperator.class.getSimpleName());
            this.outputMemoryContext = localAggregatedMemoryContext
                    .newLocalMemoryContext(ScanFilterAndProjectOmniOperator.class.getSimpleName());
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.tableScanNodeOptional = tableScanNodeOptional;
            this.stateStoreProviderOptional = stateStoreProviderOptional;
            this.queryIdOptional = queryIdOptional;
            this.metadataOptional = metadataOptional;
            this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;
            this.context = context;
        }

        // this method mimics the logic from PageSourceManager.createPageSource(..), with the exception that it check if the
        // underlying/real PageSourceProvider has a specialized implementation that returns OmniBlock Page that we can leverage.
        private ConnectorPageSource createPageSourceForSplit(Split split)
        {
            requireNonNull(columns, "columns is null");
            checkArgument(split.getCatalogName().equals(table.getCatalogName()), "mismatched split and table");
            CatalogName catalogName = split.getCatalogName();

            ConnectorPageSourceProvider provider = pageSourceProvider.getPageSourceProvider(catalogName);

            try {
                Class<? extends ConnectorPageSourceProvider> pageSourceProviderCls = provider.getClass();
                Method createOmniPageSourceMethod = pageSourceProviderCls.getMethod("createOmniPageSource",
                        ConnectorTransactionHandle.class,
                        ConnectorSession.class,
                        ConnectorSplit.class,
                        ConnectorTableHandle.class,
                        List.class,
                        Optional.class);
                Object object = createOmniPageSourceMethod.invoke(provider,
                        table.getTransaction(),
                        session.toConnectorSession(catalogName),
                        split.getConnectorSplit(),
                        table.getConnectorHandle(),
                        columns,
                        dynamicFilter);
                if (object instanceof ConnectorPageSource) {
                    return (ConnectorPageSource) object;
                }
            }
            catch (Exception e) {
                // we log at debug level here, since it is optional for connector page source to implement  "createOmniPageSource"
                if (log.isDebugEnabled()) {
                    log.debug("Attempt to use createOmniPageSource failed: %s", e.getMessage());
                }
                // let it fall through, and let fallback to regular page source
            }

            if (!dynamicFilter.isPresent()) {
                return provider.createPageSource(
                        table.getTransaction(),
                        session.toConnectorSession(catalogName),
                        split.getConnectorSplit(),
                        table.getConnectorHandle(),
                        columns);
            }
            else {
                return provider.createPageSource(
                        table.getTransaction(),
                        session.toConnectorSession(catalogName),
                        split.getConnectorSplit(),
                        table.getConnectorHandle(),
                        columns,
                        dynamicFilter);
            }
        }

        @Override
        public TransformationState<WorkProcessor<Page>> process(Split split)
        {
            if (split == null) {
                memoryContext.close();
                return finished();
            }

            checkState(cursor == null && pageSource == null, "Table scan split already set");

            ConnectorPageSource source;
            if (split.getConnectorSplit() instanceof EmptySplit) {
                source = new EmptySplitPageSource();
            }
            else {
                source = createPageSourceForSplit(split);
            }

            if (source instanceof RecordPageSource) {
                cursor = ((RecordPageSource) source).getCursor();
                return ofResult(processColumnSource());
            }
            else {
                pageSource = source;
                return ofResult(processPageSource());
            }
        }

        WorkProcessor<Page> processColumnSource()
        {
            return WorkProcessor.create(new RecordCursorToPages(session, yieldSignal, cursorProcessor, types,
                            pageSourceMemoryContext, outputMemoryContext, tableScanNodeOptional, stateStoreProviderOptional,
                            queryIdOptional, metadataOptional, dynamicFilterCacheManagerOptional)).yielding(yieldSignal::isSet)
                    .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }

        WorkProcessor<Page> processPageSource()
        {
            omniMergePagesTransformation = new OmniMergePages.OmniMergePagesTransformation(types, minOutputPageSize.toBytes(),
                    minOutputPageRowCount, MAX_MIN_PAGE_SIZE, localAggregatedMemoryContext.newLocalMemoryContext(MergePages.class.getSimpleName()), context);
            return WorkProcessor
                    .create(new ConnectorPageSourceToPages(
                            pageSourceMemoryContext, tableScanNodeOptional, stateStoreProviderOptional, queryIdOptional,
                            metadataOptional, dynamicFilterCacheManagerOptional))
                    .yielding(yieldSignal::isSet)
                    .flatMap(page -> pageProcessor.createWorkProcessor(session.toConnectorSession(), yieldSignal,
                            outputMemoryContext, page))
                    .transformProcessor(processor -> processor.transform(omniMergePagesTransformation))
                    .withProcessStateMonitor(state -> memoryContext.setBytes(localAggregatedMemoryContext.getBytes()));
        }
    }

    // Table scan operators do not participate in snapshotting
    @RestorableConfig(unsupported = true)
    private class RecordCursorToPages
            implements WorkProcessor.Process<Page>
    {
        final Session session;
        final DriverYieldSignal yieldSignal;
        final CursorProcessor cursorProcessor;
        final PageBuilder pageBuilder;
        final LocalMemoryContext pageSourceMemoryContext;
        final LocalMemoryContext outputMemoryContext;
        final Optional<TableScanNode> tableScanNodeOptional;
        final Optional<StateStoreProvider> stateStoreProviderOptional;
        final Optional<QueryId> queryIdOptional;
        final Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
        final Optional<Metadata> metadataOptional;
        Map<String, byte[]> bloomFiltersBackup = new HashMap<>();
        Map<Integer, BloomFilter> bloomFilters = new ConcurrentHashMap<>();
        boolean existsCrossFilter;
        boolean isDcTable;

        boolean finished;
        private final List<Type> outputTypes;

        RecordCursorToPages(Session session, DriverYieldSignal yieldSignal, CursorProcessor cursorProcessor,
                            List<Type> types, LocalMemoryContext pageSourceMemoryContext, LocalMemoryContext outputMemoryContext,
                            Optional<TableScanNode> tableScanNodeOptional, Optional<StateStoreProvider> stateStoreProviderOptional,
                            Optional<QueryId> queryIdOptional, Optional<Metadata> metadataOptional,
                            Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional)
        {
            this.session = session;
            this.yieldSignal = yieldSignal;
            this.cursorProcessor = cursorProcessor;
            this.pageBuilder = new PageBuilder(types);
            this.pageSourceMemoryContext = pageSourceMemoryContext;
            this.outputMemoryContext = outputMemoryContext;
            this.tableScanNodeOptional = tableScanNodeOptional;
            this.stateStoreProviderOptional = stateStoreProviderOptional;
            this.queryIdOptional = queryIdOptional;
            this.metadataOptional = metadataOptional;
            this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;

            if (queryIdOptional.isPresent() && stateStoreProviderOptional.isPresent()
                    && stateStoreProviderOptional.get().getStateStore() != null && metadataOptional.isPresent()
                    && tableScanNodeOptional.isPresent()) {
                existsCrossFilter = true;

                if (DataCenterUtility.isDCCatalog(metadataOptional.get(),
                        tableScanNodeOptional.get().getTable().getCatalogName().getCatalogName())) {
                    isDcTable = true;
                }
            }
            this.outputTypes = types;
        }

        @Override
        public ProcessState<Page> process()
        {
            if (!finished) {
                CursorProcessorOutput output = cursorProcessor.process(session.toConnectorSession(), yieldSignal,
                        cursor, pageBuilder);
                pageSourceMemoryContext.setBytes(cursor.getSystemMemoryUsage());

                processedPositions += output.getProcessedRows();

                processedBytes = cursor.getCompletedBytes();
                physicalBytes = cursor.getCompletedBytes();
                readTimeNanos = cursor.getReadTimeNanos();
                if (output.isNoMoreRows()) {
                    finished = true;
                }
            }

            if (pageBuilder.isFull() || (finished && !pageBuilder.isEmpty())) {
                // only return a page if buffer is full or cursor has finished
                Page page = pageBuilder.build();

                // pull bloomFilter from stateStore and filter page
                if (existsCrossFilter) {
                    try {
                        page = filter(page);
                    }
                    catch (Throwable e) {
                        // ignore
                        log.error("Filter page error: %s", e.getMessage());
                    }
                }
                pageBuilder.reset();
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                page = transferToOffHeapPages(vecAllocator, page, outputTypes);
                return ProcessState.ofResult(page);
            }
            else if (finished) {
                checkState(pageBuilder.isEmpty());
                return ProcessState.finished();
            }
            else {
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ProcessState.yield();
            }
        }

        private Page filter(Page page)
        {
            Page input = page;
            if (bloomFilters.isEmpty()) {
                BloomFilterUtils.updateBloomFilter(queryIdOptional, isDcTable, stateStoreProviderOptional,
                        tableScanNodeOptional, dynamicFilterCacheManagerOptional, bloomFiltersBackup, bloomFilters);
            }
            if (!bloomFilters.isEmpty()) {
                input = BloomFilterUtils.filter(input, bloomFilters);
            }
            return input;
        }
    }

    // Table scan operators do not participate in snapshotting
    @RestorableConfig(unsupported = true)
    private class ConnectorPageSourceToPages
            implements WorkProcessor.Process<Page>
    {
        final LocalMemoryContext pageSourceMemoryContext;
        final Optional<StateStoreProvider> stateStoreProviderOptional;
        final Optional<TableScanNode> tableScanNodeOptional;
        final Optional<QueryId> queryIdOptional;
        final Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional;
        final Optional<Metadata> metadataOptional;
        Map<String, byte[]> bloomFiltersBackup = new HashMap<>();
        Map<Integer, BloomFilter> bloomFilters = new ConcurrentHashMap<>();
        boolean existsCrossFilter;
        boolean isDcTable;

        ConnectorPageSourceToPages(LocalMemoryContext pageSourceMemoryContext,
                                   Optional<TableScanNode> tableScanNodeOptional, Optional<StateStoreProvider> stateStoreProviderOptional,
                                   Optional<QueryId> queryIdOptional, Optional<Metadata> metadataOptional,
                                   Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional)
        {
            this.pageSourceMemoryContext = pageSourceMemoryContext;
            this.stateStoreProviderOptional = stateStoreProviderOptional;
            this.tableScanNodeOptional = tableScanNodeOptional;
            this.queryIdOptional = queryIdOptional;
            this.metadataOptional = metadataOptional;
            this.dynamicFilterCacheManagerOptional = dynamicFilterCacheManagerOptional;
            if (queryIdOptional.isPresent() && stateStoreProviderOptional.isPresent()
                    && stateStoreProviderOptional.get().getStateStore() != null && metadataOptional.isPresent()
                    && tableScanNodeOptional.isPresent()) {
                existsCrossFilter = true;

                if (DataCenterUtility.isDCCatalog(metadataOptional.get(),
                        tableScanNodeOptional.get().getTable().getCatalogName().getCatalogName())) {
                    isDcTable = true;
                }
            }
        }

        @Override
        public ProcessState<Page> process()
        {
            if (pageSource.isFinished()) {
                return ProcessState.finished();
            }

            CompletableFuture<?> isBlocked = pageSource.isBlocked();
            if (!isBlocked.isDone()) {
                return ProcessState.blocked(toListenableFuture(isBlocked));
            }

            Page page = pageSource.getNextPage();
            pageSourceMemoryContext.setBytes(pageSource.getSystemMemoryUsage());

            if (page == null) {
                if (pageSource.isFinished()) {
                    return ProcessState.finished();
                }
                else {
                    return ProcessState.yield();
                }
            }

            page = recordMaterializedBytes(page, sizeInBytes -> processedBytes += sizeInBytes);

            // update operator stats
            processedPositions += page.getPositionCount();
            physicalBytes = pageSource.getCompletedBytes();
            readTimeNanos = pageSource.getReadTimeNanos();

            // pull bloomFilter from stateStore and filter page
            if (existsCrossFilter) {
                try {
                    page = filter(page);
                }
                catch (Throwable e) {
                    // ignore
                    log.error("Filter page error: %s", e.getMessage());
                }
            }
            page = transferToOffHeapPages(vecAllocator, page, inputTypes);
            return ProcessState.ofResult(page);
        }

        private Page filter(Page page)
        {
            Page input = page;
            if (bloomFilters.isEmpty()) {
                BloomFilterUtils.updateBloomFilter(queryIdOptional, isDcTable, stateStoreProviderOptional,
                        tableScanNodeOptional, dynamicFilterCacheManagerOptional, bloomFiltersBackup, bloomFilters);
            }
            if (!bloomFilters.isEmpty()) {
                input = BloomFilterUtils.filter(input, bloomFilters);
            }
            return input;
        }
    }

    public static class ScanFilterAndProjectOmniOperatorFactory
            extends AbstractOmniOperatorFactory
            implements SourceOperatorFactory, WorkProcessorSourceOperatorFactory
    {
        private final Supplier<CursorProcessor> cursorProcessor;
        private final Supplier<PageProcessor> pageProcessor;
        private final PlanNodeId sourceId;
        private final PageSourceProvider pageSourceProvider;
        private final TableHandle table;
        private final List<ColumnHandle> columns;
        private final Optional<DynamicFilterSupplier> dynamicFilter;
        private final List<Type> types;
        private final DataSize minOutputPageSize;
        private final int minOutputPageRowCount;
        private boolean closed;
        private Optional<TableScanNode> tableScanNodeOptional = Optional.empty();
        private Optional<StateStoreProvider> stateStoreProviderOptional = Optional.empty();
        private Optional<QueryId> queryIdOptional = Optional.empty();
        private Optional<Metadata> metadataOptional = Optional.empty();
        private Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional = Optional.empty();
        private final ReuseExchangeOperator.STRATEGY strategy;
        private final UUID reuseTableScanMappingId;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private final Integer spillerThreshold;
        private final Integer consumerTableScanNodeCount;
        private VecAllocator vecAllocator = VecAllocator.GLOBAL_VECTOR_ALLOCATOR;

        private OmniLocalExecutionPlanner.OmniLocalExecutionPlanContext context;

        public ScanFilterAndProjectOmniOperatorFactory(Session session, int operatorId, PlanNodeId planNodeId,
                                                       PlanNode sourceNode, PageSourceProvider pageSourceProvider, Supplier<CursorProcessor> cursorProcessor,
                                                       Supplier<PageProcessor> pageProcessor, TableHandle table, Iterable<ColumnHandle> columns,
                                                       Optional<DynamicFilterSupplier> dynamicFilter, List<Type> types, StateStoreProvider stateStoreProvider,
                                                       Metadata metadata, DynamicFilterCacheManager dynamicFilterCacheManager, DataSize minOutputPageSize,
                                                       int minOutputPageRowCount, ReuseExchangeOperator.STRATEGY strategy, UUID reuseTableScanMappingId,
                                                       boolean spillEnabled, Optional<SpillerFactory> spillerFactory, Integer spillerThreshold, Integer consumerTableScanNodeCount, List<Type> inputTypes,
                                                       OmniLocalExecutionPlanContext context)
        {
            this(session, operatorId, planNodeId, sourceNode, pageSourceProvider, cursorProcessor, pageProcessor, table,
                    columns, dynamicFilter, types, stateStoreProvider, metadata, dynamicFilterCacheManager, minOutputPageSize, minOutputPageRowCount, strategy,
                    reuseTableScanMappingId, spillEnabled, spillerFactory, spillerThreshold, consumerTableScanNodeCount,
                    inputTypes);
            this.context = context;
        }

        public ScanFilterAndProjectOmniOperatorFactory(Session session, int operatorId, PlanNodeId planNodeId,
                                                       PlanNode sourceNode, PageSourceProvider pageSourceProvider, Supplier<CursorProcessor> cursorProcessor,
                                                       Supplier<PageProcessor> pageProcessor, TableHandle table, Iterable<ColumnHandle> columns,
                                                       Optional<DynamicFilterSupplier> dynamicFilter, List<Type> types, StateStoreProvider stateStoreProvider,
                                                       Metadata metadata, DynamicFilterCacheManager dynamicFilterCacheManager, DataSize minOutputPageSize,
                                                       int minOutputPageRowCount, ReuseExchangeOperator.STRATEGY strategy, UUID reuseTableScanMappingId,
                                                       boolean spillEnabled, Optional<SpillerFactory> spillerFactory, Integer spillerThreshold, Integer consumerTableScanNodeCount, List<Type> sourceTypes)
        {
            this(operatorId, planNodeId, sourceNode.getId(), pageSourceProvider, cursorProcessor, pageProcessor, table,
                    columns, dynamicFilter, types, minOutputPageSize, minOutputPageRowCount, strategy,
                    reuseTableScanMappingId, spillEnabled, spillerFactory, spillerThreshold, consumerTableScanNodeCount,
                    sourceTypes);

            if (isCrossRegionDynamicFilterEnabled(session)) {
                if (sourceNode instanceof TableScanNode) {
                    this.tableScanNodeOptional = Optional.of((TableScanNode) sourceNode);
                }
                if (stateStoreProvider != null) {
                    stateStoreProviderOptional = Optional.of(stateStoreProvider);
                }
                this.queryIdOptional = Optional.of(session.getQueryId());
                this.metadataOptional = Optional.of(metadata);
                this.dynamicFilterCacheManagerOptional = Optional.of(dynamicFilterCacheManager);
            }
        }

        public ScanFilterAndProjectOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, PlanNodeId sourceId,
                                                       PageSourceProvider pageSourceProvider, Supplier<CursorProcessor> cursorProcessor,
                                                       Supplier<PageProcessor> pageProcessor, TableHandle table, Iterable<ColumnHandle> columns,
                                                       Optional<DynamicFilterSupplier> dynamicFilter, List<Type> types, DataSize minOutputPageSize,
                                                       int minOutputPageRowCount, ReuseExchangeOperator.STRATEGY strategy, UUID reuseTableScanMappingId,
                                                       boolean spillEnabled, Optional<SpillerFactory> spillerFactory, Integer spillerThreshold,
                                                       Integer consumerTableScanNodeCount, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.cursorProcessor = requireNonNull(cursorProcessor, "cursorProcessor is null");
            this.pageProcessor = requireNonNull(pageProcessor, "pageProcessor is null");
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.table = requireNonNull(table, "table is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.dynamicFilter = dynamicFilter;
            this.types = requireNonNull(types, "types is null");
            this.minOutputPageSize = requireNonNull(minOutputPageSize, "minOutputPageSize is null");
            this.minOutputPageRowCount = minOutputPageRowCount;
            this.strategy = strategy;
            this.reuseTableScanMappingId = reuseTableScanMappingId;
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.spillerThreshold = spillerThreshold;
            this.consumerTableScanNodeCount = consumerTableScanNodeCount;
            this.sourceTypes = sourceTypes;
            checkDataTypes(this.sourceTypes);
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public String getOperatorType()
        {
            return ScanFilterAndProjectOmniOperator.class.getSimpleName();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    getOperatorType());
            VecAllocator vecAllocator = VecAllocatorHelper.createOperatorLevelAllocator(driverContext,
                    VecAllocator.UNLIMIT, ScanFilterAndProjectOmniOperator.class);
            this.vecAllocator = vecAllocator != null ? vecAllocator : VecAllocator.GLOBAL_VECTOR_ALLOCATOR;
            return new WorkProcessorSourceOperatorAdapter(operatorContext, this, strategy, reuseTableScanMappingId,
                    spillEnabled, types, spillerFactory, spillerThreshold, consumerTableScanNodeCount);
        }

        @Override
        public WorkProcessorSourceOperator create(Session session, MemoryTrackingContext memoryTrackingContext, DriverYieldSignal yieldSignal, WorkProcessor<Split> splits)
        {
            return new ScanFilterAndProjectOmniOperator(session, memoryTrackingContext, yieldSignal, splits,
                    pageSourceProvider, cursorProcessor.get(), pageProcessor.get(), table, columns, dynamicFilter,
                    types, minOutputPageSize, minOutputPageRowCount, this.tableScanNodeOptional,
                    this.stateStoreProviderOptional, queryIdOptional, metadataOptional,
                    dynamicFilterCacheManagerOptional, vecAllocator, sourceTypes, context);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }
}
