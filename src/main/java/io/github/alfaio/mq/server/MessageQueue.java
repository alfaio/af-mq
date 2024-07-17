package io.github.alfaio.mq.server;

import io.github.alfaio.mq.model.AfMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author LinMF
 * @since 2024/7/17
 **/
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    private static final String TEST_TOPIC = "io.github.alfaio.test";

    static {
        queues.putIfAbsent(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subs = new HashMap<>();

    private String topic;
    private AfMessage<?>[] queue = new AfMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(AfMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public AfMessage<?> recv(int ind) {
        if (0 <= ind && ind <= index) {
            return queue[ind];
        }
        return null;
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subs.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(String consumerId) {
        subs.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) return;
        messageQueue.unsubscribe(consumerId);
    }

    public static int send(String topic, String consumerId, AfMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static AfMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subs.containsKey(consumerId))
            throw new RuntimeException("subscription not found for topic/consumerId = "
                    + topic + "/" + consumerId);
        int offset = messageQueue.subs.get(consumerId).getOffset();
        return messageQueue.recv(offset);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subs.containsKey(consumerId))
            throw new RuntimeException("subscription not found for topic/consumerId = "
                    + topic + "/" + consumerId);
        MessageSubscription subscription = messageQueue.subs.get(consumerId);
        if (offset > subscription.getOffset() && offset <= messageQueue.index) {
            subscription.setOffset(offset);
            return offset;
        }
        return -1;
    }
}
