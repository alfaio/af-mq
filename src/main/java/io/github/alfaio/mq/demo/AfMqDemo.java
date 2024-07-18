package io.github.alfaio.mq.demo;

import com.alibaba.fastjson.JSON;
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
        String topic = "io.github.alfaio.test";
        AfBroker broker = AfBroker.getDefault();
        AfProducer<String> producer = broker.createProducer();
        AfConsumer<Order> consumer = broker.createConsumer(topic);
        consumer.listen(topic, message -> {
            System.out.println("onMessage ok =>" + message);
        });

//        AfConsumer<?> consumer1 = broker.createConsumer(topic);
//        consumer1.sub(topic);

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "item" + ids, 100 * ids);
            producer.send(topic, new AfMessage<>(ids++, JSON.toJSONString(order), null));
        }

//        for (int i = 0; i < 10; i++) {
//            AfMessage<String> message = (AfMessage<String>)consumer1.recv(topic);
//            System.out.println(message);
//            consumer1.ack(topic, message);
//        }
        while (true) {
            char c = (char) System.in.read();
//            if (c == 'q' || c == 'e') {
//                consumer1.unsub(topic);
//                break;
//            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100 * ids);
                producer.send(topic, new AfMessage<>(ids++, JSON.toJSONString(order), null));
                System.out.println("send ok =>" + order);
            }
//            if (c == 'c') {
//                AfMessage<String> message = (AfMessage<String>)consumer1.recv(topic);
//                if (message == null) {
//                    System.out.println("consumer fail: not message");
//                }else {
//                    System.out.println("consumer ok =>" + message);
//                    consumer1.ack(topic, message);
//                }
//            }
//            if (c == 'a') {
//                while (true) {
//                    AfMessage<String> message = (AfMessage<String>)consumer1.recv(topic);
//                    if (message == null) {
//                        System.out.println("consumer fail: not message");
//                        break;
//                    }
//                    System.out.println("consumer ok =>" + message);
//                    consumer1.ack(topic, message);
//                }
//            }
        }

    }
}
