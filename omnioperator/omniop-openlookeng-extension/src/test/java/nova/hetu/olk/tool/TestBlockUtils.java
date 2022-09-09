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
import nova.hetu.omniruntime.vector.VecAllocator;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static nova.hetu.olk.tool.BlockUtils.compactVec;
import static org.mockito.Matchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.testng.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({VecAllocator.class,
        Vec.class,
        BlockUtils.class
})
@SuppressStaticInitializationFor({"nova.hetu.omniruntime.vector.VecAllocator",
        "nova.hetu.omniruntime.vector.Vec"
})
@PowerMockIgnore("javax.management.*")
public class TestBlockUtils
        extends PowerMockTestCase
{
    BooleanVec booleanVec;
    BooleanVec booleanVecRegion;
    IntVec intVec;
    IntVec intVecRegion;
    ShortVec shortVec;
    ShortVec shortVecRegion;
    LongVec longVec;
    LongVec longVecRegion;
    DoubleVec doubleVec;
    DoubleVec doubleVecRegion;
    VarcharVec varcharVec;
    VarcharVec varcharVecRegion;
    Decimal128Vec decimal128Vec;
    Decimal128Vec decimal128VecRegion;

    @BeforeMethod
    public void setUp() throws Exception
    {
        mockSupports();
    }

    @Test
    public void testVecCompact()
    {
        assertEquals(booleanVec, compactVec(booleanVec, 0, 4));
        assertEquals(booleanVecRegion, compactVec(booleanVec, 1, 2));

        assertEquals(intVec, compactVec(intVec, 0, 4));
        assertEquals(intVecRegion, compactVec(intVec, 1, 2));

        assertEquals(shortVec, compactVec(shortVec, 0, 4));
        assertEquals(shortVecRegion, compactVec(shortVec, 1, 2));

        assertEquals(longVec, compactVec(longVec, 0, 4));
        assertEquals(longVecRegion, compactVec(longVec, 1, 2));

        assertEquals(doubleVec, compactVec(doubleVec, 0, 4));
        assertEquals(doubleVecRegion, compactVec(doubleVec, 1, 2));

        assertEquals(varcharVec, compactVec(varcharVec, 0, 4));
        assertEquals(varcharVecRegion, compactVec(varcharVec, 1, 2));

        assertEquals(decimal128Vec, compactVec(decimal128Vec, 0, 4));
        assertEquals(decimal128VecRegion, compactVec(decimal128Vec, 1, 2));
    }

    private void mockSupports() throws Exception
    {
        booleanVec = mock(BooleanVec.class);
        whenNew(BooleanVec.class).withAnyArguments().thenReturn(booleanVec);
        when(booleanVec.getSize()).thenReturn(4);
        when(booleanVec.getOffset()).thenReturn(0);
        booleanVecRegion = mock(BooleanVec.class);
        when(booleanVec.copyRegion(anyInt(), anyInt())).thenReturn(booleanVecRegion);

        intVec = mock(IntVec.class);
        whenNew(IntVec.class).withAnyArguments().thenReturn(intVec);
        when(intVec.getSize()).thenReturn(4);
        when(intVec.getOffset()).thenReturn(0);
        intVecRegion = mock(IntVec.class);
        when(intVec.copyRegion(anyInt(), anyInt())).thenReturn(intVecRegion);

        shortVec = mock(ShortVec.class);
        whenNew(ShortVec.class).withAnyArguments().thenReturn(shortVec);
        when(shortVec.getSize()).thenReturn(4);
        when(shortVec.getOffset()).thenReturn(0);
        shortVecRegion = mock(ShortVec.class);
        when(shortVec.copyRegion(anyInt(), anyInt())).thenReturn(shortVecRegion);

        longVec = mock(LongVec.class);
        whenNew(LongVec.class).withAnyArguments().thenReturn(longVec);
        when(longVec.getSize()).thenReturn(4);
        when(longVec.getOffset()).thenReturn(0);
        longVecRegion = mock(LongVec.class);
        when(longVec.copyRegion(anyInt(), anyInt())).thenReturn(longVecRegion);

        doubleVec = mock(DoubleVec.class);
        whenNew(DoubleVec.class).withAnyArguments().thenReturn(doubleVec);
        when(doubleVec.getSize()).thenReturn(4);
        when(doubleVec.getOffset()).thenReturn(0);
        doubleVecRegion = mock(DoubleVec.class);
        when(doubleVec.copyRegion(anyInt(), anyInt())).thenReturn(doubleVecRegion);

        varcharVec = mock(VarcharVec.class);
        whenNew(VarcharVec.class).withAnyArguments().thenReturn(varcharVec);
        when(varcharVec.getSize()).thenReturn(4);
        when(varcharVec.getOffset()).thenReturn(0);
        varcharVecRegion = mock(VarcharVec.class);
        when(varcharVec.copyRegion(anyInt(), anyInt())).thenReturn(varcharVecRegion);

        decimal128Vec = mock(Decimal128Vec.class);
        whenNew(Decimal128Vec.class).withAnyArguments().thenReturn(decimal128Vec);
        when(decimal128Vec.getSize()).thenReturn(4);
        when(decimal128Vec.getOffset()).thenReturn(0);
        decimal128VecRegion = mock(Decimal128Vec.class);
        when(decimal128Vec.copyRegion(anyInt(), anyInt())).thenReturn(decimal128VecRegion);
    }
}
