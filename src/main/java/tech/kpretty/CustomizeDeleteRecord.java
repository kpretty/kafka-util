package tech.kpretty;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import lombok.Data;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import tech.kpretty.entity.DeleteMetaJson;
import tech.kpretty.util.CommandLineUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author wjun
 * @date 2022/6/1 10:17
 * @email wj2247689442@gmail.com
 * @describe
 */
@Data
public class CustomizeDeleteRecord {
    private static final String DEFAULT_CONSUMER = "consumer-delete-topic";

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            // 定义参数
            ArgumentAcceptingOptionSpec<String> server = parser.accepts("bootstrap-server", "REQUIRED: server(s) to use for bootstrapping.")
                    .withRequiredArg()
                    .describedAs("ip1:port,ip2:port...")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<String> topics = parser.accepts("topic", "REQUIRED: need delete topic(s)")
                    .withRequiredArg()
                    .describedAs("topic1,topic2,...")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<Long> timestamp = parser.accepts("timestamp", "REQUIRED: Delete the data before this timestamp")
                    .withRequiredArg()
                    .describedAs("1654052290746")
                    .ofType(Long.class);

            ArgumentAcceptingOptionSpec<String> output = parser.accepts("output", "REQUIRED: File output path")
                    .withRequiredArg()
                    .describedAs("/tmp")
                    .ofType(String.class);

            OptionSpecBuilder help = parser.accepts("help", "Print this message.");

            // 解析参数
            OptionSet parse = parser.parse(args);

            // 是否有 --help
            if (parse.has(help)) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }

            // 校验参数
            CommandLineUtils.checkRequest(parser, parse, server, topics, timestamp, output);

            // 获取参数
            String bootstrapServer = parse.valueOf(server);
            String[] subTopics = parse.valueOf(topics).split(",");
            Long subTs = parse.valueOf(timestamp);
            String outputPath = parse.valueOf(output);

            // 获取主题分区
            List<TopicPartition> topicPartitions = buildTopicPartition(bootstrapServer, Arrays.asList(subTopics));

            // 获取分区指定时间的偏移量
            String strategy = buildStrategy(bootstrapServer, topicPartitions, subTs);

            // 输出策略
            JSONObject strategyJson = JSONObject.parseObject(strategy);

            // json 格式化输出
            strategy = JSON.toJSONString(strategyJson, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                    SerializerFeature.WriteDateUseDateFormat);

            System.out.println(strategy);

            // 输出删除策略
            outPutFile(outputPath, strategy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取待删除 topic 的分区信息
     *
     * @param server kafka server ip:port
     * @param topics need delete topic
     * @return topic and partition infos
     */
    private static List<TopicPartition> buildTopicPartition(String server, List<String> topics) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        // 构建 kafka admin 客户端
        AdminClient client = KafkaAdminClient.create(properties);
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        try {
            // 获取指定 topic 的信息
            client.describeTopics(topics).all().get().values().forEach(value -> {
                String topic = value.name();
                value.partitions().forEach(partition -> topicPartitions.add(new TopicPartition(topic, partition.partition())));
            });
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        client.close();
        return topicPartitions;
    }

    /**
     * @param server             kafka server ip:port
     * @param topicAndPartitions topic and partition infos
     * @param timestamp          delete the data before this timestamp
     * @return delete strategy json string
     */
    private static String buildStrategy(String server, List<TopicPartition> topicAndPartitions, long timestamp) {
        ArrayList<DeleteMetaJson.TopicPartitionOffsetMeta> metas = new ArrayList<>();

        // kafka 连接信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_CONSUMER);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        HashMap<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        topicAndPartitions.forEach(topicPartition -> timestampsToSearch.put(topicPartition, timestamp));
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(timestampsToSearch);
        topicPartitionOffsetAndTimestampMap.forEach((topicPartition, offsetAndTimestamp) -> {
            if (offsetAndTimestamp != null) {
                metas.add(
                        DeleteMetaJson.of(topicPartition.topic(), topicPartition.partition(), offsetAndTimestamp.offset())
                );
            }
        });

        consumer.close();

        return new DeleteMetaJson(metas).toString();
    }


    private static void outPutFile(String outPut, String strategy) {
        File file = new File(outPut);
        if (!file.isDirectory()) {
            throw new RuntimeException(outPut + " no exist or not a directory ");
        }

        file = new File(file, "delete-strategy.json");
        BufferedWriter bufferedWriter = null;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(file, false));
            bufferedWriter.write(strategy);
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
