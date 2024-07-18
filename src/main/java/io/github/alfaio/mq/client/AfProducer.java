package io.github.alfaio.mq.client;

import io.github.alfaio.mq.model.AfMessage;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfProducer<T> {

    AfBroker broker;

    public AfProducer(AfBroker broker) {
        this.broker = broker;
    }

    public boolean send(String topic, AfMessage<T> message) {
        return broker.sand(topic, message);
    }
}
