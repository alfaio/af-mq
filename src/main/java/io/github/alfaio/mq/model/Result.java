package io.github.alfaio.mq.model;

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
public class Result<T> {

    private int code;
    private T data;


    public static Result<String> ok() {
        return new Result<>(1, "ok");
    }

    public static Result<String> ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<AfMessage<?>> msg(String msg) {
        return new Result<>(1, AfMessage.create(msg, null));
    }

    public static Result<AfMessage<?>> msg(AfMessage<?> msg) {
        return new Result<>(1, msg);
    }

}
