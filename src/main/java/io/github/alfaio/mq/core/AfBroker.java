package io.github.alfaio.mq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfBroker {

    Map<String, AfMq<?>> mqMapping = new ConcurrentHashMap<>(64);
    public AfMq<?> find(String topic) {
        return mqMapping.get(topic);
    }

    public void createTopic(String topic) {
        mqMapping.putIfAbsent(topic, new AfMq<>(topic));
    }
    public <T> AfProducer<T> createProducer() {
        return new AfProducer<>(this);
    }

    public <T> AfConsumer<T> createConsumer(String topic) {
        AfConsumer<T> consumer = new AfConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

}
