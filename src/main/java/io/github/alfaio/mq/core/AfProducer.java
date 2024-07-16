package io.github.alfaio.mq.core;

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
        AfMq<T> mq = (AfMq<T>) broker.find(topic);
        if (mq == null) throw new RuntimeException("topic not found");
        return mq.sand(message);
    }
}
