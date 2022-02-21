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
package ru.datamart.prostore.query.execution.core.base.configuration;

import ru.datamart.prostore.query.execution.core.base.configuration.properties.ServiceDbZookeeperProperties;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperConnectionProvider;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.impl.ZookeeperConnectionProviderImpl;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.impl.ZookeeperExecutorImpl;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ZookeeperConfiguration {

    @Bean("serviceDbZkConnectionProvider")
    public ZookeeperConnectionProvider serviceDbZkConnectionManager(ServiceDbZookeeperProperties properties,
                                                                    @Value("${core.env.name}") String envName) {
        return new ZookeeperConnectionProviderImpl(properties, envName);
    }

    @Bean
    public ZookeeperExecutor zookeeperExecutor(ZookeeperConnectionProvider connectionManager, Vertx vertx) {
        return new ZookeeperExecutorImpl(connectionManager, vertx);
    }


}
