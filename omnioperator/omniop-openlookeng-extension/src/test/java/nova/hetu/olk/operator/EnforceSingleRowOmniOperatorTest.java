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
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.block.ByteArrayOmniBlock;
import nova.hetu.olk.operator.EnforceSingleRowOmniOperator.EnforceSingleRowOmniOperatorFactory;
import nova.hetu.omniruntime.vector.VecAllocator;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static nova.hetu.olk.mock.MockUtil.mockPage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@PrepareForTest({
        DistinctLimitOmniOperator.class,
        ByteArrayOmniBlock.class
})
public class EnforceSingleRowOmniOperatorTest
        extends AbstractOperatorTest
{
    private final int operatorId = new Random().nextInt();
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final List<Type> sourceTypes = Arrays.asList();

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new EnforceSingleRowOmniOperatorFactory(operatorId, planNodeId, sourceTypes);
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
        return new EnforceSingleRowOmniOperator(originalOperator.getOperatorContext(), mock(VecAllocator.class));
    }

    @Override
    protected Page[] pageForTest()
    {
        return new Page[]{mockPage(), PageBuilder.withMaxPageSize(1, Collections.emptyList()).build()};
    }

    @Test(dataProvider = "pageProvider")
    public void testProcess(int i)
    {
        Operator operator = getOperator();
        Page page = getPageForTest(i);
        if (page == null) {
            assertThrows("page is null", NullPointerException.class, () -> operator.addInput(page));
        }
        else {
            operator.addInput(page);
            assertNull(operator.getOutput());
        }

        assertFalse(operator.isFinished());
        operator.finish();
        operator.getOutput();
        if (page != null) {
            assertTrue(operator.isFinished());
        }
        assertFalse(operator.needsInput());
    }
}
