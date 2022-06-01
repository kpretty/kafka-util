package tech.kpretty.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author wjun
 * @date 2022/6/1 10:18
 * @email wj2247689442@gmail.com
 * @describe 删除策略 POJO 类
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeleteMetaJson {
    private List<TopicPartitionOffsetMeta> partition;
    private static final int version = 1;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TopicPartitionOffsetMeta {
        private String topic;
        private int partition;
        private long offset;

        @Override
        public String toString() {
            return "{\"topic\":" + "\"" + topic + "\"," + "\"partition\":" + partition + "," + "\"offset\":" + offset + "}";
        }
    }

    public static TopicPartitionOffsetMeta of(String topic, int partition, long offset) {
        return new TopicPartitionOffsetMeta(topic, partition, offset);
    }

    @Override
    public String toString() {
        return "{\"partitions\":" + partition +
                ",\"version\":" + version + "}";
    }
}
