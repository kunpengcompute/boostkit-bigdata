/*
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

package io.prestosql.plugin.hive;

import com.huawei.boostkit.omnidata.reader.DataReader;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.plugin.hive.util.PageSourceUtil;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;

import java.util.OptionalLong;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_OPERATOR_OFFLOAD_FAIL;
import static java.util.Objects.requireNonNull;

public class HivePushDownPageSource
        implements ConnectorPageSource
{
    private final DataReader<Page> dataReader;
    private final AggregatedMemoryContext systemMemoryContext;

    private boolean closed;
    private long readTimeNanos;

    public HivePushDownPageSource(
            DataReader<Page> dataReader,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.dataReader = requireNonNull(dataReader, "dataReader is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return dataReader.getReadSizeInBytes();
    }

    @Override
    public OptionalLong getCompletedPositionCount()
    {
        return OptionalLong.of(dataReader.getReadPositionCount());
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        long start = System.nanoTime();

        if (dataReader.isFinished()) {
            close();
            return null;
        }

        Page page = null;
        try {
            page = dataReader.getNextPageBlocking();
        }
        catch (Exception exception) {
            PageSourceUtil.closeWithSuppression(this, exception);
            throw new PrestoException(HIVE_OPERATOR_OFFLOAD_FAIL, exception.getMessage());
        }

        readTimeNanos += System.nanoTime() - start;

        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            dataReader.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
