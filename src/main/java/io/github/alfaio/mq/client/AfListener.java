package io.github.alfaio.mq.client;

import io.github.alfaio.mq.model.AfMessage;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public interface AfListener<T> {

    void onMessage(AfMessage<T> message);
}
