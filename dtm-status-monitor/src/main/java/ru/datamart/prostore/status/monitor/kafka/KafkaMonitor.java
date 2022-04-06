/*
 * Copyright © 2022 DATAMART LLC
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
package ru.datamart.prostore.status.monitor.kafka;

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.status.kafka.StatusRequest;
import ru.datamart.prostore.common.status.kafka.StatusResponse;
import ru.datamart.prostore.status.monitor.config.AppProperties;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
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
    private static final int POLL_DURATION_MILLIS = 100;

    private final KafkaConsumer<byte[], byte[]> offsetsConsumer;
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

        offsetsConsumer = new KafkaConsumer<>(consumerProperties);
    }

    @SneakyThrows
    public StatusResponse status(StatusRequest request) {
        return collectInfo(request);
    }

    private StatusResponse collectInfo(StatusRequest request) {
        val topic = request.getTopic();
        val consumerGroup = request.getConsumerGroup();

        val response = new StatusResponse();
        response.setConsumerGroup(consumerGroup);
        response.setTopic(topic);

        log.debug("Start fetching committed offsets [{}] [{}]", topic, consumerGroup);
        commitedOffsets.keySet().stream()
                .filter(p -> p.topicPartition().topic().equals(topic) && p.group().equals(consumerGroup))
                .map(commitedOffsets::get)
                .filter(Objects::nonNull)
                .forEach(offsetAndMetadata -> {
                    response.setConsumerOffset(offsetAndMetadata.offset() + response.getConsumerOffset());
                    response.setLastCommitTime(Math.max(offsetAndMetadata.commitTimestamp(), response.getLastCommitTime()));
                });
        log.info("Finish fetching committed offsets [{}] [{}], received [{}]", topic, consumerGroup, response.getConsumerOffset());

        log.debug("Fetching end offsets for topic [{}]", topic);
        val lastMessageTime = updateLatestOffsets(topic);
        response.setLastMessageTime(lastMessageTime);
        uncommittedOffsets.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topic))
                .forEach(topicPartitionLongEntry -> response.setProducerOffset(response.getProducerOffset() + topicPartitionLongEntry.getValue()));
        log.info("Finish fetching end offsets [{}], received [{}]", topic, response.getProducerOffset());

        return response;
    }

    private Properties getConsumerProperties() {
        val props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getBrokersList());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP + UUID.randomUUID());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private void startConsumer() {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(SYSTEM_TOPIC));
            while (!Thread.currentThread().isInterrupted()) {
                val consumerRecords = consumer.poll(Duration.ofMillis(POLL_DURATION_MILLIS));
                for (val consumerRecord : consumerRecords) {
                    updateOffsets(consumerRecord);
                }
            }
        } catch (Exception e) {
            log.error("Error updating offsets", e);
        }
    }

    @SneakyThrows
    private void updateOffsets(ConsumerRecord<byte[], byte[]> consumerRecord) {
        val key = consumerRecord.key();
        val value = consumerRecord.value();
        if (key == null) {
            return;
        }

        BaseKey baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
        if (baseKey instanceof OffsetKey) {
            val offsetKey = (OffsetKey) baseKey;
            val topic = offsetKey.key().topicPartition().topic();
            val consumerGroup = offsetKey.key().group();
            val partition = offsetKey.key().topicPartition().partition();
            val groupTopicPartition = new GroupTopicPartition(consumerGroup, topic, partition);

            if (value != null) {
                val offset = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value));
                commitedOffsets.put(groupTopicPartition, offset);
                log.debug("Received offset [{}] for topic [{}], partition [{}], group [{}]", offset.offset(), topic, partition, consumerGroup);
            } else {
                commitedOffsets.remove(groupTopicPartition);
                log.debug("Removed offset for topic [{}], partition [{}], group [{}]", topic, partition, consumerGroup);
            }
        }
    }

    private long updateLatestOffsets(String topic) {
        long lastMessageTime = 0;

        synchronized (offsetsConsumer) {
            val partitionsForRequestTopic = offsetsConsumer.partitionsFor(topic).stream()
                    .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                    .collect(Collectors.toList());
            offsetsConsumer.assign(partitionsForRequestTopic);
            offsetsConsumer.seekToEnd(partitionsForRequestTopic);

            partitionsForRequestTopic.forEach(tp -> {
                long lastOffset = offsetsConsumer.position(tp);

                uncommittedOffsets.put(tp, lastOffset);

                if (lastOffset > 0) {
                    offsetsConsumer.seek(tp, lastOffset - 1);
                }
            });

            val consumerRecords = offsetsConsumer.poll(Duration.ofMillis(POLL_DURATION_MILLIS));
            for (val consumerRecord : consumerRecords) {
                lastMessageTime = Math.max(consumerRecord.timestamp(), lastMessageTime);
            }

            offsetsConsumer.assign(Collections.emptyList());
        }

        return lastMessageTime;
    }

}
