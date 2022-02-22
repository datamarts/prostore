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
package ru.datamart.prostore.kafka.core.repository;

import ru.datamart.prostore.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaZookeeperConnectionProvider;
import ru.datamart.prostore.kafka.core.service.kafka.KafkaZookeeperConnectionProviderImpl;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Component("mapZkKafkaProviderRepository")
public class ZookeeperKafkaProviderRepositoryImpl implements ZookeeperKafkaProviderRepository {
    private final KafkaZookeeperProperties defaultProperties;
    private final Map<String, KafkaZookeeperConnectionProvider> zkConnProviderMap = new ConcurrentHashMap<>();

    @Override
    public KafkaZookeeperConnectionProvider getOrCreate(String connectionString) {
        val zookeeperProperties = new KafkaZookeeperProperties();
        zookeeperProperties.setConnectionString(connectionString);
        zookeeperProperties.setChroot(defaultProperties.getChroot());
        return zkConnProviderMap.compute(zookeeperProperties.getConnectionString(), (ignored, connectionProvider) -> {
            if (connectionProvider == null) {
                return new KafkaZookeeperConnectionProviderImpl(zookeeperProperties);
            }

            return connectionProvider;
        });
    }
}
