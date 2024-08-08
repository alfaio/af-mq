package io.github.alfaio.mq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LinMF
 * @since 2024/7/28
 **/
public class Indexer {
    static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();
    static Map<Integer, Entry> mappings = new HashMap<>();
    @Data
    @AllArgsConstructor
    public static class Entry {
        int offset;
        int length;
    }
    public void add(String topic, Entry entry) {
        indexes.add(topic, entry);
    }

    public static void add(String topic, int offset, int length) {
        Entry entry = new Entry(offset, length);
        indexes.add(topic, entry);
        mappings.put(offset, entry);
    }


    public static List<Entry> getAll(String topic) {
        return indexes.get(topic);
    }

    public static Entry get(String topic, Integer offset) {
        return mappings.get(offset);
    }
}
