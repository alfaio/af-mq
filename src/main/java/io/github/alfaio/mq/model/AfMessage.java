package io.github.alfaio.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AfMessage<T> {
    static AtomicLong idGen = new AtomicLong(0);
    //    private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers = new HashMap<>(); //系统属性
//    private Map<String, String> properties; // 业务属性

    public static long nextId() {
        return idGen.incrementAndGet();
    }

    public static AfMessage<?> create(String body, Map<String, String> headers) {
        return new AfMessage<>(nextId(), body, headers);
    }
}
