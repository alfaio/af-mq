package io.github.alfaio.mq.server;

import io.github.alfaio.mq.model.AfMessage;
import io.github.alfaio.mq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author LinMF
 * @since 2024/7/17
 **/
@RestController
@RequestMapping("/afmq")
public class MqServer {

    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody AfMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    @RequestMapping("/recv")
    public Result<AfMessage<?>> recv(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @RequestMapping("/batch")
    public Result<List<AfMessage<?>>> batch(@RequestParam("t") String topic,
                                           @RequestParam("cid") String consumerId,
                                           @RequestParam(value = "size", defaultValue = "1000") Integer size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }

    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    @RequestMapping("/unsub")
    public Result<String> unsubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(topic, consumerId);
        return Result.ok();
    }
}
