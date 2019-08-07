package io.github.rapid.queue.core.kit;

import java.util.UUID;

public abstract class UUIDKit {
    public static String randomUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
