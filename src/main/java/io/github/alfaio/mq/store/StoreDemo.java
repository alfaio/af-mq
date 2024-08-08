package io.github.alfaio.mq.store;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.alfaio.mq.model.AfMessage;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Scanner;

/**
 * mmap store. (memory map，也就是内存映射)
 * 
 * @author LinMF
 * @since 2024/7/27
 **/
public class StoreDemo {

    @SneakyThrows
    public static void main(String[] args) {
        String content = """
            this is a good file.
            that is a new line for store.
            """;
        int length = content.length();
        System.out.println("content length: " + length);
        File file = new File("test.dat");
        if(!file.exists()) file.createNewFile();
        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + " -> " + mapped.position());
                AfMessage<String> msg = (AfMessage<String>) AfMessage.create(i + ":" + content, null);
                System.out.println("msg id: " + msg.getId());
                String json = JSON.toJSONString(msg);
                Indexer.add("test", mapped.position(), json.getBytes(StandardCharsets.UTF_8).length);
                mapped.put(StandardCharsets.UTF_8.encode(json));
            }

            length += 2;

            ByteBuffer readOnlyBuffer = mapped.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                if (line.equals("q")) break;
                System.out.println(" IN = " + line);
                int id = Integer.parseInt(line);
                Indexer.Entry entry = Indexer.get("test", id);
                readOnlyBuffer.position(entry.getOffset());
                byte[] bytes = new byte[entry.getLength()];
                readOnlyBuffer.get(bytes, 0, length);
                String s = new String(bytes, StandardCharsets.UTF_8);
                AfMessage<String> message = JSON.parseObject(s, new TypeReference<AfMessage<String>>() {
                });
                System.out.println(" OUT = " + message.getBody());
            }
        }
    }

}
