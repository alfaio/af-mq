package io.github.alfaio.mq.server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author LinMF
 * @since 2024/7/17
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageSubscription {
    private String topic;
    private String consumerId;
    private int offset = -1;
}
