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
package io.prestosql.plugin.hive.parquet;

import com.huawei.boostkit.omnidata.model.datasource.DataSource;
import com.huawei.boostkit.omnidata.reader.DataReader;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.util.PageSourceUtil;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_OPERATOR_OFFLOAD_FAIL;

public class ParquetPushDownPageSource
        implements ConnectorPageSource
{
    private final DataReader dataReader;
    private final DataSource dataSource;
    private boolean closed;
    private final AggregatedMemoryContext systemMemoryContext;
    private final FileFormatDataSourceStats stats;
    private long readTimeNanos;
    private long readBytes;

    public ParquetPushDownPageSource(DataReader reader, DataSource dataSource, AggregatedMemoryContext systemMemoryContext, FileFormatDataSourceStats stats)
    {
        this.dataReader = reader;
        this.dataSource = dataSource;
        this.stats = stats;
        this.systemMemoryContext = systemMemoryContext;
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes;
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
            page = (Page) dataReader.getNextPageBlocking();
        }
        catch (Exception e) {
            PageSourceUtil.closeWithSuppression(this, e);
            throw new PrestoException(HIVE_OPERATOR_OFFLOAD_FAIL, e.getMessage());
        }

        readTimeNanos += System.nanoTime() - start;
        if (page != null) {
            readBytes += page.getSizeInBytes();
        }

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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataSource", dataSource.toString())
                .toString();
    }
}
