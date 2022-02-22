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
package ru.datamart.prostore.query.execution.plugin.adg.base.configuration;

import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.AdgWebClientProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.CircuitBreakerProperties;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class VertxConfiguration {

    @Bean("adgWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx,
                               AdgWebClientProperties properties) {
        return WebClient.create(vertx, properties);
    }

    @Bean("adgCircuitBreaker")
    public CircuitBreaker circuitBreaker(@Qualifier("coreVertx") Vertx vertx,
                                         @Qualifier("adgCircuitBreakerProperties") CircuitBreakerProperties properties) {
        return CircuitBreaker.create("adgCircuitBreaker", vertx,
                new CircuitBreakerOptions()
                        .setMaxFailures(properties.getMaxFailures())
                        .setTimeout(properties.getTimeout())
                        .setFallbackOnFailure(properties.isFallbackOnFailure())
                        .setResetTimeout(properties.getResetTimeout()));
    }

}

