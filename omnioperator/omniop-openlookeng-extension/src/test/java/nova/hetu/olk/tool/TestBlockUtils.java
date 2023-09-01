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

package nova.hetu.olk.tool;

import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Vec.class,
        BlockUtils.class
})
@SuppressStaticInitializationFor("nova.hetu.omniruntime.vector.Vec")
@PowerMockIgnore("javax.management.*")
public class TestBlockUtils
        extends PowerMockTestCase
{
    BooleanVec booleanVec;
    IntVec intVec;
    ShortVec shortVec;
    LongVec longVec;
    DoubleVec doubleVec;
    VarcharVec varcharVec;
    Decimal128Vec decimal128Vec;

    @BeforeMethod
    public void setUp() throws Exception
    {
        mockSupports();
    }

    private void mockSupports() throws Exception
    {
        booleanVec = mock(BooleanVec.class);
        whenNew(BooleanVec.class).withAnyArguments().thenReturn(booleanVec);
        when(booleanVec.getSize()).thenReturn(4);

        intVec = mock(IntVec.class);
        whenNew(IntVec.class).withAnyArguments().thenReturn(intVec);
        when(intVec.getSize()).thenReturn(4);

        shortVec = mock(ShortVec.class);
        whenNew(ShortVec.class).withAnyArguments().thenReturn(shortVec);
        when(shortVec.getSize()).thenReturn(4);

        longVec = mock(LongVec.class);
        whenNew(LongVec.class).withAnyArguments().thenReturn(longVec);
        when(longVec.getSize()).thenReturn(4);

        doubleVec = mock(DoubleVec.class);
        whenNew(DoubleVec.class).withAnyArguments().thenReturn(doubleVec);
        when(doubleVec.getSize()).thenReturn(4);

        varcharVec = mock(VarcharVec.class);
        whenNew(VarcharVec.class).withAnyArguments().thenReturn(varcharVec);
        when(varcharVec.getSize()).thenReturn(4);

        decimal128Vec = mock(Decimal128Vec.class);
        whenNew(Decimal128Vec.class).withAnyArguments().thenReturn(decimal128Vec);
        when(decimal128Vec.getSize()).thenReturn(4);
    }
}
