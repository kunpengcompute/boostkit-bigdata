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

import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.block.LazyOmniBlock;
import nova.hetu.olk.operator.DistinctLimitOmniOperator.DistinctLimitOmniOperatorFactory;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.operator.limit.OmniDistinctLimitOperatorFactory;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static java.util.Arrays.asList;
import static nova.hetu.olk.mock.MockUtil.mockNewWithWithAnyArguments;
import static nova.hetu.olk.mock.MockUtil.mockOmniOperator;
import static nova.hetu.olk.mock.MockUtil.mockPage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@PrepareForTest({
        DistinctLimitOmniOperator.class,
        OperatorUtils.class,
        LazyOmniBlock.class
})
public class DistinctLimitOmniOperatorTest
        extends AbstractOperatorTest
{
    private int operatorId = new Random().nextInt();
    private PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private List<Type> sourceTypes = Collections.emptyList();
    private List<Integer> distinctChannels = Collections.emptyList();
    private Optional<Integer> hashChannel = Optional.empty();
    private long limit = Math.abs(new Random().nextLong() + 1);

    @Override
    protected void setUpMock()
    {
        super.setUpMock();
        mockNewWithWithAnyArguments(OmniDistinctLimitOperatorFactory.class);
    }

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new DistinctLimitOmniOperatorFactory(operatorId, planNodeId, sourceTypes, distinctChannels, hashChannel,
                limit);
    }

    @Override
    protected void checkOperatorFactory(OperatorFactory operatorFactory)
    {
        super.checkOperatorFactory(operatorFactory);
        assertEquals(operatorFactory.getSourceTypes(), sourceTypes);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new DistinctLimitOmniOperator(originalOperator.getOperatorContext(), mockOmniOperator(), sourceTypes,
                distinctChannels.stream().mapToInt(Integer::intValue).toArray(),
                hashChannel.orElse(0), limit);
    }

    @Override
    protected Page[] pageForTest()
    {
        return new Page[]{
                mockPage(),
                PageBuilder.withMaxPageSize(1, asList()).build(),
                new Page(new LazyBlock(10, block -> {}), new LazyBlock(10, block -> {}))
        };
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        assertTrue(operator.needsInput());

        if (page == null) {
            assertThrows("page is null", NullPointerException.class, () -> operator.addInput(page));
        }
        else {
            operator.addInput(page);
        }

        assertFalse(operator.isFinished());
        operator.finish();
        operator.getOutput();
        if (page != null) {
            assertTrue(operator.isFinished());
        }
        assertNull(operator.getOutput());
        assertFalse(operator.needsInput());
    }
}
