package io.github.alfaio.mq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.alfaio.mq.model.AfMessage;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author LinMF
 * @since 2024/8/7
 **/
public class Store {

    private final String topic;
    public static final int LEN = 1024 * 1024;

    @Getter
    MappedByteBuffer mapped;

    public Store(String topic) {
        this.topic = topic;
        this.mapped = this.init(topic);
    }

    @SneakyThrows
    public MappedByteBuffer init(String topic) {
        File file = new File(topic + ".dat");
        if (!file.exists()) file.createNewFile();
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);
        return mapped;
        // todo: 读取索引
        // 判断是否有数据
        // 找到数据结尾
        // mapped.setPosition(X);
        // TODO: 如果总数据 > 10M, 使用多个数据文件的list来管理持久化数据
        // 创建第二个数据文件，怎么来管理多个数据文件
    }

    public int write(AfMessage<String> msg) {
        int position = mapped.position();
        System.out.println("write pos -> " + position);
        String json = JSON.toJSONString(msg);
        int length = json.getBytes(StandardCharsets.UTF_8).length;
        String format = String.format("%010d", length);
        json = format + json;
        length = length + 10;
        Indexer.add(topic, position, length);
        mapped.put(StandardCharsets.UTF_8.encode(json));
        return position;
    }

    public int pos() {
        return mapped.position();
    }
    public AfMessage<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mapped.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.get(topic, offset);
        readOnlyBuffer.position(entry.getOffset());
        int length = entry.getLength();
        byte[] bytes = new byte[length];
        readOnlyBuffer.get(bytes, 0, length);
        String json = new String(bytes, StandardCharsets.UTF_8);
        AfMessage<String> message = JSON.parseObject(json, new TypeReference<AfMessage<String>>() {
        });
        return message;
    }
}
