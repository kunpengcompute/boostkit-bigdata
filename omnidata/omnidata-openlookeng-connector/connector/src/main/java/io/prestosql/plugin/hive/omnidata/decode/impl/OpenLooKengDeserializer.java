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

package io.prestosql.plugin.hive.omnidata.decode.impl;

import com.huawei.boostkit.omnidata.decode.Deserializer;
import io.airlift.compress.Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * Deserialize block
 *
 * @since 2022-07-18
 */
public class OpenLooKengDeserializer
        implements Deserializer<Page>
{
    private final Decompressor decompressor;
    private final OpenLooKengDecoding decoding;

    /**
     * Constructor of deserialize block
     */
    public OpenLooKengDeserializer()
    {
        decoding = new OpenLooKengDecoding();
        decompressor = new ZstdDecompressor();
    }

    /**
     * Decompress serialized page
     *
     * @param page page need decompress
     * @param decompressor decompressor
     * @return Slice decompressed
     */
    public static Slice decompressPage(SerializedPage page, Decompressor decompressor)
    {
        if (!page.isCompressed()) {
            return page.getSlice();
        }
        Slice slice = page.getSlice();
        int uncompressedSize = page.getUncompressedSizeInBytes();
        byte[] decompressed = new byte[uncompressedSize];
        if (slice.getBase() instanceof byte[]) {
            byte[] sliceBase = (byte[]) slice.getBase();
            checkState(
                    decompressor.decompress(
                            sliceBase,
                            (int) (slice.getAddress() - ARRAY_BYTE_BASE_OFFSET),
                            slice.length(),
                            decompressed,
                            0,
                            uncompressedSize) == uncompressedSize);
        }

        return Slices.wrappedBuffer(decompressed);
    }

    @Override
    public Page deserialize(SerializedPage page)
    {
        checkArgument(page != null, "page is null");

        if (page.isEncrypted()) {
            throw new UnsupportedOperationException("unsupported encrypted page.");
        }

        Slice slice = decompressPage(page, decompressor);
        SliceInput input = slice.getInput();
        int numberOfBlocks = input.readInt();
        Block<?>[] blocks = new Block<?>[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = decoding.decode(Optional.empty(), input);
        }

        return new Page(page.getPositionCount(), page.getPageMetadata(), blocks);
    }
}
