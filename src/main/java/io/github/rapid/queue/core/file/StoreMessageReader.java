package io.github.rapid.queue.core.file;


import io.github.rapid.queue.core.RapidQueueMessage;
import io.github.rapid.queue.core.kit.ImperfectException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

class StoreMessageReader implements Iterable<RapidQueueMessage>, AutoCloseable {

    private final int startPageId;
    @Nullable
    private final Integer startPos;
    private final int endPageId;
    private final int endPos;
    private final StoreMessageHelper storeMessageHelper;
    //
    private StorePageReader pageReader;
    private Iterator<StorePageReaderFrame> frameIterable;

    StoreMessageReader(int startPageId, @Nullable Integer startPos, int endPageId, int endPos, StoreMessageHelper storeMessageHelper) throws IOException {
        this.startPageId = startPageId;
        this.startPos = startPos;
        this.endPageId = endPageId;
        this.endPos = endPos;
        this.storeMessageHelper = storeMessageHelper;

        nextPage();
    }

    private boolean nextPage() throws IOException {
        if (this.pageReader == null) {
            this.pageReader = StorePageReader.createOpened(startPageId
                    , storeMessageHelper
                    , startPageId == endPageId ? endPos : null
            );
            this.frameIterable = pageReader.readFull(startPos).iterator();
            return true;
        } else {
            final StorePageReader pageReader = this.pageReader;
            int nextPage = pageReader.pageId + 1;
            if (nextPage > endPageId) {
                return false;
            } else {
                this.pageReader = StorePageReader.createOpened(nextPage
                        , storeMessageHelper
                        , nextPage == endPageId ? endPos : null
                );
                this.frameIterable = this.pageReader.readFull(null).iterator();
                pageReader.close();
                return true;
            }
        }
    }


    @Nonnull
    @Override
    public Iterator<RapidQueueMessage> iterator() {
        return new Iterator<RapidQueueMessage>() {
            @Override
            public boolean hasNext() {
                try {
                    if (frameIterable.hasNext()) {
                        return true;
                    } else {
                        return nextPage() && frameIterable.hasNext();
                    }
                } catch (Exception e) {
                    throw ImperfectException.pack(pageReader.pageId, e.getMessage(), e);
                }
            }

            @Override
            public RapidQueueMessage next() {
                StorePageReaderFrame frame = frameIterable.next();
                return new RapidQueueMessage(
                        StoreBase.createOffset(frame.pageId, frame.position)
                        , frame.frameMessage.getPayload()
                        , true
                );
            }
        };
    }


    @Override
    public void close() throws IOException {
        if (pageReader != null) {
            pageReader.close();
        }
    }
}
