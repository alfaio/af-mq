package io.github.alfaio.mq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.alfaio.mq.model.AfMessage;
import io.github.alfaio.mq.model.Result;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
public class AfBroker {

    @Getter
    public static AfBroker Default = new AfBroker();

    public static final String BROKER_URL = "http://localhost:8765/afmq";

    static {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, AfConsumer<?>> consumers = getDefault().getConsumers();
            consumers.forEach((topic, consumers1) -> {
                consumers1.forEach(consumer -> {
                    AfMessage<?> message = consumer.recv(topic);
                    if (message == null) return;
                    try {
                        consumer.getListener().onMessage(message);
                        consumer.ack(topic, (AfMessage<String>) message);
                    } catch (Exception e) {
                        // todo
                    }
                });
            });
        }, 100, 100);
    }

    public <T> AfProducer<T> createProducer() {
        return new AfProducer<>(this);
    }

    public <T> AfConsumer<T> createConsumer(String topic) {
        AfConsumer<T> consumer = new AfConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public <T> boolean sand(String topic, AfMessage<T> message) {
        System.out.println(" ===>>> send topic/message: " + topic + "/" + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
                BROKER_URL + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        System.out.println(" ===>>> send result: " + result);
        return result.getCode() == 1;
    }

    public void sub(String topic, String cid) {
        System.out.println(" ===>>> send topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(BROKER_URL + "/sub?t=" + topic
                + "&cid=" + cid, new TypeReference<Result<String>>() {
        });
        System.out.println(" ===>>> sub result: " + result);
    }

    public void unsub(String topic, String cid) {
        System.out.println(" ===>>> unsub topic/cid: " + topic + "/" + cid);
        Result<String> result = HttpUtils.httpGet(BROKER_URL + "/unsub?t=" + topic
                + "&cid=" + cid, new TypeReference<Result<String>>() {
        });
        System.out.println(" ===>>> unsub result: " + result);
    }

    public <T> AfMessage<T> recv(String topic, String cid) {
        System.out.println(" ===>>> recv topic/cid: " + topic + "/" + cid);
        Result<AfMessage<String>> result = HttpUtils.httpGet(BROKER_URL + "/recv?t=" + topic
                + "&cid=" + cid, new TypeReference<Result<AfMessage<String>>>() {
        });
        System.out.println(" ===>>> recv result: " + result);
        return (AfMessage<T>) result.getData();
    }

    public boolean ack(String topic, String cid, int offset) {
        System.out.println(" ===>>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
        Result<String> result = HttpUtils.httpGet(BROKER_URL + "/ack?t=" + topic + "&cid="
                + cid + "&offset=" + offset, new TypeReference<Result<String>>() {
        });
        System.out.println(" ===>>> ack result: " + result);
        return result.getCode() == 1;
    }

    @Getter
    private final MultiValueMap<String, AfConsumer<?>> consumers = new LinkedMultiValueMap<>();

    public <T> void addConsumer(String topic, AfConsumer<T> consumer) {
        consumers.add(topic, consumer);
    }

}
