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

package nova.hetu.olk.operator.filterandproject;

import io.airlift.units.DataSize;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.operator.AbstractOperatorTest;
import nova.hetu.olk.operator.filterandproject.FilterAndProjectOmniOperator.FilterAndProjectOmniOperatorFactory;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterAndProjectOmniOperatorTest
        extends AbstractOperatorTest
{
    private final PlanNodeId planNodeId = new PlanNodeId(UUID.randomUUID().toString());
    private final Supplier<PageProcessor> processor = () -> {
        PageProcessor processor = mock(PageProcessor.class);
        when(processor.process(any(), any(), any(), any())).thenReturn(new ArrayListIterator());
        return processor;
    };
    private final List<Type> types = Collections.emptyList();
    private final DataSize minOutputPageSize = new DataSize(1, DataSize.Unit.BYTE);
    private final int minOutputPageRowCount = 0;

    @Override
    protected OperatorFactory createOperatorFactory()
    {
        return new FilterAndProjectOmniOperatorFactory(0, planNodeId, processor, types, minOutputPageSize,
                minOutputPageRowCount);
    }

    @Override
    protected Operator createOperator(Operator originalOperator)
    {
        return new FilterAndProjectOmniOperator(originalOperator.getOperatorContext(), processor.get(),
                new OmniMergingPageOutput(types, minOutputPageSize.toBytes(), minOutputPageRowCount));
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
        }

        assertFalse(operator.isFinished());
        operator.finish();
        operator.getOutput();
        assertTrue(operator.isFinished());
        assertFalse(operator.needsInput());
    }
}
