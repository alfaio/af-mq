package io.github.alfaio.mq.core;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public interface AfListener<T> {

    void onMessage(AfMessage<T> message);
}
