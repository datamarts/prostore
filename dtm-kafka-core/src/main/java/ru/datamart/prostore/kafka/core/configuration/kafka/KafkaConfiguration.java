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
package ru.datamart.prostore.kafka.core.configuration.kafka;


import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.kafka.core.configuration.properties.KafkaProperties;
import ru.datamart.prostore.kafka.core.factory.KafkaProducerFactory;
import ru.datamart.prostore.kafka.core.factory.impl.VertxKafkaProducerFactory;
import ru.datamart.prostore.kafka.core.repository.ZookeeperKafkaProviderRepository;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.datamart.prostore.kafka.core.service.kafka.RestConsumerMonitorImpl;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


@Configuration
@DependsOn({"coreKafkaProperties", "mapZkKafkaProviderRepository"})
public class KafkaConfiguration {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String BROKERS_DELIMITER = ",";

    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(@Qualifier("mapZkKafkaProviderRepository")
                                                                             ZookeeperKafkaProviderRepository zkConnProviderRepo,
                                                                     KafkaZookeeperProperties kafkaZkProperties,
                                                                     KafkaProperties kafkaProperties,
                                                                     @Qualifier("coreVertx") Vertx vertx) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        String kafkaBrokers =
                zkConnProviderRepo.getOrCreate(kafkaZkProperties.getConnectionString()).getKafkaBrokers().stream()
                        .map(KafkaBrokerInfo::getAddress)
                        .collect(Collectors.joining(BROKERS_DELIMITER));
        kafkaPropertyMap.put(BOOTSTRAP_SERVERS, kafkaBrokers);
        return new VertxKafkaProducerFactory<>(vertx, kafkaPropertyMap);
    }

    @Bean("coreKafkaConsumerMonitor")
    public KafkaConsumerMonitor kafkaConsumerMonitor(@Qualifier("coreVertx") Vertx vertx,
                                                     KafkaProperties kafkaProperties) {
        return new RestConsumerMonitorImpl(vertx, kafkaProperties);
    }

    @ConditionalOnProperty(
            value = "core.kafka.status.event.publish.enabled",
            havingValue = "true"
    )
    @Bean("jsonCoreKafkaProducer")
    public KafkaProducer<String, String> jsonCoreKafkaProducer(@Qualifier("coreKafkaProducerFactory") KafkaProducerFactory<String, String> producerFactory,
                                                               @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties,
                                                               @Qualifier("mapZkKafkaProviderRepository")
                                                                       ZookeeperKafkaProviderRepository zkConnProviderRepo,
                                                               KafkaZookeeperProperties kafkaZkProperties) {
        Map<String, String> kafkaPropertyMap = new HashMap<>(kafkaProperties.getProducer().getProperty());
        String kafkaBrokers =
                zkConnProviderRepo.getOrCreate(kafkaZkProperties.getConnectionString()).getKafkaBrokers().stream()
                        .map(KafkaBrokerInfo::getAddress)
                        .collect(Collectors.joining(BROKERS_DELIMITER));
        kafkaPropertyMap.put(BOOTSTRAP_SERVERS, kafkaBrokers);
        return producerFactory.create(kafkaPropertyMap);
    }
}
