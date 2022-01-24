/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.status.monitor.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.status.monitor.config.AppProperties;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
@Slf4j
public class KafkaMonitor {
    private static final String SYSTEM_TOPIC = "__consumer_offsets";
    private static final String CONSUMER_GROUP = "kafka.status.monitor";

    private final KafkaConsumer<byte[], byte[]> offsetProvider;
    private final KafkaConsumer<byte[], byte[]> lastMessageTimeProvider;
    private final AppProperties appProperties;
    private final ExecutorService consumerService;
    private final Properties consumerProperties;

    private final ConcurrentHashMap<TopicPartition, Long> uncommittedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GroupTopicPartition, OffsetAndMetadata> commitedOffsets = new ConcurrentHashMap<>();

    public KafkaMonitor(AppProperties appProperties) {
        this.appProperties = appProperties;

        consumerProperties = getConsumerProperties();

        consumerService = Executors.newFixedThreadPool(appProperties.getConsumersCount());
        IntStream.range(0, appProperties.getConsumersCount()).forEach(i -> consumerService.submit(this::startConsumer));

        offsetProvider = new KafkaConsumer<>(consumerProperties);
        offsetProvider.subscribe(Collections.singletonList(SYSTEM_TOPIC));

        lastMessageTimeProvider = new KafkaConsumer<>(consumerProperties);
    }

    @SneakyThrows
    public StatusResponse status(StatusRequest request) {
        return collectInfo(request);
    }

    public List<StatusResponse> listAll() {
        return null;
    }

    private StatusResponse collectInfo(StatusRequest request) {
        StatusResponse response = new StatusResponse();
        response.setConsumerGroup(request.getConsumerGroup());
        response.setTopic(request.getTopic());

        // make a local copy of current kafka state
        List<GroupTopicPartition> partitions = commitedOffsets.keySet().stream()
            .filter(p -> p.topicPartition().topic().equals(request.getTopic()) &&
                p.group().equals(request.getConsumerGroup()))
            .collect(Collectors.toList());

        response.setLastMessageTime(getLastMessageTime(request.getTopic()));

        log.debug("Fetching end offsets");
        updateLatestOffsets(request.getTopic());
        val endOffsets = uncommittedOffsets.entrySet().stream()
            .filter(e -> e.getKey().topic().equals(request.getTopic()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        endOffsets.forEach((tp, offset) -> response.setProducerOffset(response.getProducerOffset() + offset));
        log.info(String.format("Finish fetching end offsets, received %d", endOffsets.entrySet().size()));

        partitions.forEach(tp -> {
            OffsetAndMetadata offset = commitedOffsets.get(tp);
            response.setConsumerOffset(offset.offset() + response.getConsumerOffset());
            response.setLastCommitTime(Math.max(offset.commitTimestamp(), response.getLastCommitTime()));
        });

        return response;
    }

    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBrokersList());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP + UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private void startConsumer() {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(SYSTEM_TOPIC));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                try {
                    updateOffsets(record);
                } catch (Exception e) {
                    log.error("Error parse message", e);
                }
            }
        }
    }

    @SneakyThrows
    private void updateOffsets(ConsumerRecord<byte[], byte[]> record) {
        byte[] key = record.key();
        byte[] value = record.value();
        if (key == null || value == null) {
            return;
        }

        BaseKey baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
        if (baseKey instanceof OffsetKey) {
            OffsetKey offsetKey = (OffsetKey) baseKey;
            String topic = offsetKey.key().topicPartition().topic();
            String consumerGroup = offsetKey.key().group();
            int partition = offsetKey.key().topicPartition().partition();

            OffsetAndMetadata offset = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value));
            // Because all OffsetKey messages for specified group, topic and partition are placed into one partition,
            // so only one Consumer thread will read and update them.
            // We replay all messages from specified partition in chronological order, and we can perform simple update by key
            commitedOffsets.put(new GroupTopicPartition(consumerGroup, topic, partition), offset);
            log.debug(String.format("Received offset %d for topic %s, partition %d, group %s", offset.offset(),
                topic,
                partition,
                consumerGroup));
        }
    }

    private void updateLatestOffsets(String topicName) {
        try {
            synchronized (offsetProvider) {
                List<TopicPartition> topicPartitions = offsetProvider.partitionsFor(topicName).stream()
                    .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                    .collect(Collectors.toList());
                Map<TopicPartition, Long> topicPartitionLongMap = offsetProvider.endOffsets(topicPartitions);
                uncommittedOffsets.putAll(topicPartitionLongMap);
            }
        } catch (Exception e) {
            log.error("Error updating last offsets for subscribed topic of {} ", topicName, e);
        }
    }

    private long getLastMessageTime(String topic) {
        long lastMessageTime = 0;

        synchronized (lastMessageTimeProvider) {
            List<TopicPartition> partitionsForRequestTopic = lastMessageTimeProvider.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
            lastMessageTimeProvider.assign(partitionsForRequestTopic);
            lastMessageTimeProvider.seekToEnd(partitionsForRequestTopic);

            partitionsForRequestTopic.forEach(tp -> {
                long lastOffset = lastMessageTimeProvider.position(tp);
                if (lastOffset > 0) {
                    lastMessageTimeProvider.seek(tp, lastOffset - 1);
                }
            });

            ConsumerRecords<byte[], byte[]> records = lastMessageTimeProvider.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                lastMessageTime = Math.max(record.timestamp(), lastMessageTime);
            }
        }
        return lastMessageTime;
    }

}
