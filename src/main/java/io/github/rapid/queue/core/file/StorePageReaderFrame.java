package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.MessageFrame;

final class StorePageReaderFrame {
    final int pageId;
    final int position;
    final MessageFrame messageFrame;

    StorePageReaderFrame(int pageId, int position, MessageFrame messageFrame) {
        this.pageId = pageId;
        this.position = position;
        this.messageFrame = messageFrame;
    }
}
