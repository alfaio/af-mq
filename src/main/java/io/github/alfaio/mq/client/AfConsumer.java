package io.github.alfaio.mq.client;

import io.github.alfaio.mq.model.AfMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfConsumer<T> {
    String id;
    AfBroker broker;
//    String topic;

    static AtomicInteger idGen = new AtomicInteger(0);

    public AfConsumer(AfBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void sub(String topic) {
//        this.topic = topic;
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
//        this.topic = topic;
        broker.unsub(topic, id);
    }

    public AfMessage<T> recv(String topic) {
        return broker.recv(topic, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, AfMessage<String> message) {
        String offset = message.getHeaders().get("X-offset");
        return broker.ack(topic, id, Integer.parseInt(offset));
    }

    // todo
    public void listen(String topic, AfListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    @Getter
    private AfListener listener;

}
