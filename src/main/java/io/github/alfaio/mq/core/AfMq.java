package io.github.alfaio.mq.core;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfMq<T> {

    private final String topic;

    public AfMq(String topic) {
        this.topic = topic;
    }

    private final LinkedBlockingDeque<AfMessage<T>> queue = new LinkedBlockingDeque<>(1024);
    private final List<AfListener<T>> listeners = new ArrayList<>();

    public boolean sand(AfMessage<T> message) {
        boolean offered = queue.offer(message);
        if (offered) {
            listeners.forEach(listener -> listener.onMessage(message));
        }
        return offered;
    }

    /**
     * 拉模式获取消息
     *
     * @param timeout timeout
     * @return message
     */
    @SneakyThrows
    public AfMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public void addListener(AfListener<T> listener) {
        listeners.add(listener);
    }

}
