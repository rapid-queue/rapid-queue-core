package io.github.rapid.queue.core.file;

final class StorePageReaderFrame {
    final int pageId;
    final int position;
    final FrameMessage frameMessage;

    StorePageReaderFrame(int pageId, int position, FrameMessage frameMessage) {
        this.pageId = pageId;
        this.position = position;
        this.frameMessage = frameMessage;
    }
}
