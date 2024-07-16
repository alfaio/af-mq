package io.github.alfaio.mq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author LinMF
 * @since 2024/7/16
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private long id;
    private String item;
    private long price;
}
