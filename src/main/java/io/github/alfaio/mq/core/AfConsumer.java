package io.github.alfaio.mq.core;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfConsumer<T> {
    AfBroker broker;
    String topic;
    AfMq mq;

    public AfConsumer(AfBroker broker) {
        this.broker = broker;
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
