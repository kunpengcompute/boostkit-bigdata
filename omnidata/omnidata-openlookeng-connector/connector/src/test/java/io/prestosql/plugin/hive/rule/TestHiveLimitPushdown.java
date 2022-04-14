/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.plugin.hive.rule;

import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.TableScanNode;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.COLUMN_TYPE_MAP;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.ID_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.OFFLOAD_SESSION;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.SYMBOL_ALLOCATOR;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildPartialLimitNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.buildTableScanNode;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.matchLimitOffload;
import static io.prestosql.plugin.hive.rule.TestHivePushdownUtil.simulationHiveTransactionManager;

public class TestHiveLimitPushdown
        extends TestHivePushdown
{
    @Test
    public void testLimitPushdown()
    {
        int count = 5;
        HiveLimitPushdown optimizer = new HiveLimitPushdown(simulationHiveTransactionManager());
        TableScanNode tableScanNode = buildTableScanNode();
        LimitNode limitNode = buildPartialLimitNode(tableScanNode, count);
        PlanNode node = optimizer.optimize(limitNode, OFFLOAD_SESSION, COLUMN_TYPE_MAP, SYMBOL_ALLOCATOR, ID_ALLOCATOR);
        matchLimitOffload(node, count);
    }
}
