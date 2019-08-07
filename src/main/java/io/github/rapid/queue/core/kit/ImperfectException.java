package io.github.rapid.queue.core.kit;


public final class ImperfectException extends RuntimeException {
    private final int pageId;

    private ImperfectException(int pageId, String message, Throwable cause) {
        super(message, cause);
        this.pageId = pageId;
    }

    public ImperfectException(int pageId, String message) {
        super(message);
        this.pageId = pageId;
    }

    public int getPageId() {
        return pageId;
    }

    public static ImperfectException pack(int pageId, String message, Throwable cause) {
        if (cause instanceof ImperfectException) {
            return (ImperfectException) cause;
        }
        return new ImperfectException(pageId, message, cause);
    }

    @Override
    public String toString() {
        return "ImperfectException{" +
                "pageId=" + pageId +
                "} " + super.toString();
    }
}
