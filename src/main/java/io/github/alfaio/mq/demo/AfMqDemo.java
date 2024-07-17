package io.github.alfaio.mq.demo;

import io.github.alfaio.mq.client.AfBroker;
import io.github.alfaio.mq.client.AfConsumer;
import io.github.alfaio.mq.model.AfMessage;
import io.github.alfaio.mq.client.AfProducer;
import lombok.SneakyThrows;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfMqDemo {

    @SneakyThrows
    public static void main(String[] args) {
        long ids = 0;
        String topic = "af.order";
        AfBroker broker = new AfBroker();
        broker.createTopic(topic);
        AfProducer<Order> producer = broker.createProducer();
        AfConsumer<Order> consumer = broker.createConsumer(topic);
        consumer.listen((message) -> {
            System.out.println("onMessage ok =>" + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new AfMessage<>(ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            AfMessage<Order> message = consumer.poll(1000);
            System.out.println(message);
        }
        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new AfMessage<>(ids++, order, null));
                System.out.println("send ok =>" + order);
            }
            if (c == 'c') {
                AfMessage<Order> message = consumer.poll(1000);
                System.out.println("poll ok =>" + message);
            }
        }

    }
}
