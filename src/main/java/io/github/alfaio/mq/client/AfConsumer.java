package io.github.alfaio.mq.client;

import io.github.alfaio.mq.model.AfMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfConsumer<T> {
    String id;
    AfBroker broker;
    String topic;
    AfMq mq;

    static AtomicInteger idGen = new AtomicInteger(0);

    public AfConsumer(AfBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(topic);
        if (mq ==null) throw new RuntimeException("topic not found");
    }

    public AfMessage<T> poll(long timeout){
        return mq.poll(timeout);
    }

    public void listen(AfListener<T> listener) {
        mq.addListener(listener);
    }
}
