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

import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.operator.exchange.LocalExchangeSource;
import io.prestosql.operator.exchange.PageReference;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import nova.hetu.olk.tool.BlockUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;

public class OmniLocalExchangeSource
        extends LocalExchangeSource
{
    public OmniLocalExchangeSource(Consumer<LocalExchangeSource> onFinish, MultiInputSnapshotState snapshotState)
    {
        super(onFinish, snapshotState);
    }

    @Override
    public void addPage(PageReference pageReference, String origin)
    {
        checkNotHoldsLock();

        boolean added = false;
        SettableFuture<?> notEmptySettableFuture;
        synchronized (lock) {
            // ignore pages after finish
            if (!finishing) {
                if (snapshotState != null) {
                    // For local-merge
                    Page page;
                    synchronized (snapshotState) {
                        // This may look suspicious, in that if there are "pending pages" in the snapshot state, then
                        // a) those pages were associated with specific input channels (exchange source/sink) when the state
                        // was captured, but now they would be returned to any channel asking for the next page, and
                        // b) when the pending page is returned, the current page (in pageReference) is discarded and lost.
                        // But the above never happens because "merge" operators are always preceded by OrderByOperators,
                        // which only send data pages at the end, *after* all markers. That means when snapshot is taken,
                        // no data page has been received, so when the snapshot is restored, there won't be any pending pages.
                        page = snapshotState.processPage(() -> Pair.of(pageReference.peekPage(), origin)).orElse(null);
                    }
                    //if new input page is marker, we don't add it to buffer, it will be obtained through MultiInputSnapshotState's getPendingMarker()
                    if (page instanceof MarkerPage || page == null) {
                        pageReference.removePage();
                        // whenever local exchange source sees a marker page, it's going to check whether operator after local merge is blocked by it.
                        // if it is, this local exchange source will unblock in order for next operator to ask for output to pass down the marker.
                        if (!this.notEmptyFuture.isDone()) {
                            notEmptySettableFuture = this.notEmptyFuture;
                            this.notEmptyFuture = NOT_EMPTY;
                            notEmptySettableFuture.set(null);
                        }
                        return;
                    }
                }
                // buffered bytes must be updated before adding to the buffer to assure
                // the count does not go negative
                bufferedBytes.addAndGet(pageReference.getRetainedSizeInBytes());
                buffer.add(pageReference);
                originBuffer.add(Optional.ofNullable(origin));
                added = true;
            }

            // we just added a page (or we are finishing) so we are not empty
            notEmptySettableFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        if (!added) {
            // dereference the page outside of lock
            Page page = pageReference.removePage();
            BlockUtils.freePage(page);
        }

        // notify readers outside of lock since this may result in a callback
        notEmptySettableFuture.set(null);
    }

    @Override
    public void close()
    {
        checkNotHoldsLock();

        List<PageReference> remainingPages;
        SettableFuture<?> notEmptySettableFuture;
        synchronized (lock) {
            finishing = true;

            remainingPages = new ArrayList<>(buffer);
            buffer.clear();
            originBuffer.clear();
            bufferedBytes.addAndGet(-remainingPages.stream().mapToLong(PageReference::getRetainedSizeInBytes).sum());

            notEmptySettableFuture = this.notEmptyFuture;
            this.notEmptyFuture = NOT_EMPTY;
        }

        // free all the remaining pages
        for (PageReference pageReference : remainingPages) {
            Page page = pageReference.removePage();
            BlockUtils.freePage(page);
        }

        // notify readers outside of lock since this may result in a callback
        notEmptySettableFuture.set(null);

        // this will always fire the finished event
        checkState(isFinished(), "Expected buffer to be finished");
        checkFinished();
    }
}
