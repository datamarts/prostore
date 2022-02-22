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
package ru.datamart.prostore.query.execution.core.base.configuration;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import ru.datamart.prostore.common.schema.codec.AvroEncoder;

@Setter
@Configuration
public class AppConfiguration {
    private Environment environment;

    @Autowired
    public AppConfiguration(Environment environment) {
        this.environment = environment;
    }

    public String getEnvName() {
        return environment.getProperty("core.env.name", String.class);
    }

    @Bean
    public AvroEncoder avroEncoder() {
        return new AvroEncoder();
    }

    @Bean("coreWebClient")
    public WebClient webClient(@Qualifier("coreVertx") Vertx vertx) {
        return WebClient.create(vertx);
    }
}
