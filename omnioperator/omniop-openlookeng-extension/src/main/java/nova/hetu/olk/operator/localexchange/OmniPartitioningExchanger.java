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

package nova.hetu.olk.operator.localexchange;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.HashGenerator;
import io.prestosql.operator.InterpretedHashGenerator;
import io.prestosql.operator.PrecomputedHashGenerator;
import io.prestosql.operator.exchange.LocalExchangeMemoryManager;
import io.prestosql.operator.exchange.LocalExchanger;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.operator.exchange.PageReference;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import nova.hetu.olk.operator.filterandproject.OmniMergingPageOutput;
import nova.hetu.olk.tool.BlockUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static java.util.Objects.requireNonNull;

public class OmniPartitioningExchanger
        implements LocalExchanger
{
    private final List<BiConsumer<PageReference, String>> buffers;

    private final LocalExchangeMemoryManager memoryManager;

    private final LocalPartitionGenerator partitionGenerator;

    private final IntArrayList[] partitionAssignments;

    private final OmniMergingPageOutput mergingPageOutput;

    private String origin;

    public OmniPartitioningExchanger(List<BiConsumer<PageReference, String>> partitions,
                                     LocalExchangeMemoryManager memoryManager, List<? extends Type> types, List<Integer> partitionChannels,
                                     Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        HashGenerator hashGenerator;
        if (hashChannel.isPresent()) {
            hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
        }
        else {
            List<Type> partitionChannelTypes = partitionChannels.stream().map(types::get).collect(toImmutableList());
            hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels));
        }
        partitionGenerator = new LocalPartitionGenerator(hashGenerator, buffers.size());

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
        mergingPageOutput = new OmniMergingPageOutput(types, 128000, 256);
    }

    private Iterator<Optional<Page>> createPagesIterator(Page... pages)
    {
        return createPagesIterator(ImmutableList.copyOf(pages));
    }

    private Iterator<Optional<Page>> createPagesIterator(List<Page> pages)
    {
        return transform(pages.iterator(), Optional::of);
    }

    @Override
    public synchronized void accept(Page page, String origin)
    {
        this.origin = origin;
        mergingPageOutput.addInput(createPagesIterator(page));
        Page mergedPage = mergingPageOutput.getOutput();
        while (mergedPage != null) {
            process(mergedPage, origin);
            mergedPage = mergingPageOutput.getOutput();
        }
    }

    private void process(Page mergedPage, String origin)
    {
        // reset the assignment lists
        for (IntList partitionAssignment : partitionAssignments) {
            partitionAssignment.clear();
        }

        // assign each row to a partition
        for (int position = 0; position < mergedPage.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(mergedPage, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Block[] outputBlocks = new Block[mergedPage.getChannelCount()];
        for (int partition = 0; partition < buffers.size(); partition++) {
            IntArrayList positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                for (int i = 0; i < mergedPage.getChannelCount(); i++) {
                    outputBlocks[i] = mergedPage.getBlock(i).copyPositions(positions.elements(), 0, positions.size());
                }

                Page pageSplit = new Page(positions.size(), outputBlocks);
                memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1,
                        () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())), origin);
            }
        }
        BlockUtils.freePage(mergedPage);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }

    @Override
    public void finish()
    {
        mergingPageOutput.finish();
        Page mergedPage = mergingPageOutput.getOutput();
        while (mergedPage != null) {
            process(mergedPage, this.origin);
            mergedPage = mergingPageOutput.getOutput();
        }
    }
}
