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

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.Session;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.session.ResourceEstimates;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.SqlPath;
import io.prestosql.transaction.TransactionId;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.FixedWidthVec;
import nova.hetu.omniruntime.vector.LazyVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.execution.Lifespan.taskWide;
import static nova.hetu.olk.mock.MockUtil.block;
import static nova.hetu.olk.mock.MockUtil.fill;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockPage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@SuppressStaticInitializationFor({
        "nova.hetu.omniruntime.vector.VecAllocator",
        "nova.hetu.omniruntime.vector.Vec",
        "nova.hetu.omniruntime.constants.Constant",
        "nova.hetu.omniruntime.operator.OmniOperatorFactory"
})
@PowerMockIgnore("javax.management.*")
public class AbstractOperatorTest
        extends PowerMockTestCase
{
    private final QueryId queryId = new QueryId(Math.abs(new Random().nextLong()) + "");
    private final Optional<TransactionId> empty = Optional.empty();
    private final boolean clientTransactionSupport = false;
    private final Identity identity = Identity.ofUser("");
    private final Optional<String> source = Optional.empty();
    private final Optional<String> catalog = Optional.empty();
    private final Optional<String> schema = Optional.empty();
    private final SqlPath sqlPath = new SqlPath(Optional.empty());
    private final Optional<String> traceToken = Optional.empty();
    private final TimeZoneKey utcKey = TimeZoneKey.UTC_KEY;
    private final Locale locale = Locale.ROOT;
    private final Optional<String> remoteUserAddress = Optional.empty();
    private final Optional<String> userAgent = Optional.empty();
    private final Optional<String> clientInfo = Optional.empty();
    private final HashSet<String> clientTags = new HashSet<>();
    private final HashSet<String> capabilities = new HashSet<>();
    private final ResourceEstimates resourceEstimates = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());
    private final long startTime = System.currentTimeMillis();
    private final HashMap<String, String> systemProperties = new HashMap<>();
    private final HashMap<CatalogName, Map<String, String>> connectorProperties = new HashMap<>();
    private final HashMap<String, Map<String, String>> unprocessedCatalogProperties = new HashMap<>();
    private final SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
    private final HashMap<String, String> preparedStatements = new HashMap<>();
    private final boolean pageMetadataEnabled = false;

    @Mock
    private DriverContext driverContext;
    @Mock
    private PipelineContext pipelineContext;
    @Mock
    protected OperatorContext operatorContext;
    @Mock
    private TaskContext taskContext;
    @Mock
    private LocalMemoryContext localMemoryContext;
    @Mock
    private PagesSerde pagesSerde;

    private OperatorFactory operatorFactory;

    private Operator operator;

    @DataProvider(name = "pageProvider")
    public Object[][] dataProvider()
    {
        AtomicInteger atomicBiInteger = new AtomicInteger(0);
        return Arrays.stream(pageForTest()).map(page -> new Object[]{atomicBiInteger.getAndIncrement()}).toArray(Object[][]::new);
    }

    protected Page[] pageForTest()
    {
        return new Page[]{
                mockPage(),
                mockPage(block(false, false, fill(new Integer[3], index -> new Random().nextInt()))),
                mockPage(block(false, false, fill(new Integer[3], index -> new Random().nextInt())),
                        block(false, false, fill(new String[3], index -> UUID.randomUUID().toString())))
        };
    }

    protected final Page getPageForTest(int i)
    {
        return pageForTest()[i];
    }

    @BeforeMethod
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        when(pipelineContext.getTaskContext()).thenReturn(taskContext);
        when(taskContext.getTaskExtendProperties()).thenReturn(new HashMap<>());
        when(taskContext.getTaskId()).thenReturn(new TaskId("1.2.3"));
        when(driverContext.getPipelineContext()).thenReturn(pipelineContext);
        when(driverContext.addOperatorContext(anyInt(), any(), anyString())).thenReturn(operatorContext);
        when(driverContext.getLifespan()).thenReturn(taskWide());
        when(driverContext.getSerde()).thenReturn(pagesSerde);
        when(driverContext.getYieldSignal()).thenReturn(new DriverYieldSignal());
        when(operatorContext.aggregateRevocableMemoryContext()).thenReturn(AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
        when(operatorContext.aggregateUserMemoryContext()).thenReturn(AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
        when(operatorContext.aggregateSystemMemoryContext()).thenReturn(AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
        when(operatorContext.newLocalSystemMemoryContext(anyString())).thenReturn(localMemoryContext);
        when(operatorContext.getDriverContext()).thenReturn(driverContext);
        when(operatorContext.getSession()).thenReturn(createSession());
        when(operatorContext.getUniqueId()).thenReturn(UUID.randomUUID().toString());
        when(pagesSerde.serialize(any())).thenReturn(SerializedPage.forMarker(MarkerPage.snapshotPage(1)));
        setUpMock();
        operatorFactory = createOperatorFactory();
        checkOperatorFactory(operatorFactory);
        operator = createOperator(operatorFactory.createOperator(driverContext));
        checkOperator(operator);
    }

    protected void setUpMock()
    {
        VecBatch vecBatch = mockNewWithWithAnyArguments(VecBatch.class);
        when(vecBatch.getVectors()).thenReturn(new Vec[0]);
        mockNewWithWithAnyArguments(LazyVec.class);
        mockNewWithWithAnyArguments(Vec.class);
        mockNewWithWithAnyArguments(BooleanVec.class);
        mockNewWithWithAnyArguments(FixedWidthVec.class);
    }

    protected OperatorFactory createOperatorFactory()
    {
        return null;
    }

    protected Operator createOperator(Operator originalOperator)
    {
        return originalOperator;
    }

    protected final Operator getOperator()
    {
        return operator;
    }

    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        assertEquals(isExtensionFactory(), operatorFactory.isExtensionOperatorFactory());
        if (throwWhenDuplicate() == null) {
            operatorFactory.duplicate();
        }
        else {
            assertThrows(UnsupportedOperationException.class, operatorFactory::duplicate);
        }
    }

    protected void checkOperator(Operator operator)
    {
        assertEquals(operator.getOperatorContext(), operatorContext);
    }

    protected boolean isExtensionFactory()
    {
        return true;
    }

    protected Class<? extends Throwable> throwWhenDuplicate()
    {
        return null;
    }

    private Session createSession()
    {
        return new Session(queryId, empty, clientTransactionSupport, identity, source, catalog, schema, sqlPath,
                traceToken, utcKey, locale, remoteUserAddress, userAgent, clientInfo, clientTags, capabilities,
                resourceEstimates, startTime, systemProperties, connectorProperties, unprocessedCatalogProperties,
                sessionPropertyManager, preparedStatements, pageMetadataEnabled);
    }

    @AfterMethod
    public void destroy()
    {
        operatorFactory.noMoreOperators(Lifespan.taskWide());
        operatorFactory.noMoreOperators();
        try {
            operator.close();
        }
        catch (Exception ignore) {
        }
    }
}
