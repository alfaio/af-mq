package io.github.alfaio.mq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AfMessage<T> {
    //    private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers; //系统属性
//    private Map<String, String> properties; // 业务属性
}
