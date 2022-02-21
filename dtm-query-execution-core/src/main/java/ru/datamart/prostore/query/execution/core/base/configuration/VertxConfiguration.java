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

import ru.datamart.prostore.query.execution.core.base.configuration.properties.VertxCoreProperties;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.VertxPoolProperties;
import ru.datamart.prostore.query.execution.core.init.service.CoreInitializationService;
import ru.datamart.prostore.query.execution.core.query.utils.LoggerContextUtils;
import io.reactiverse.contextual.logging.ContextualData;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
public class VertxConfiguration implements ApplicationListener<ApplicationReadyEvent> {

    @Bean("coreVertx")
    @ConditionalOnMissingBean(Vertx.class)
    public Vertx vertx(VertxPoolProperties poolProperties, VertxCoreProperties coreProperties) {
        VertxOptions options = new VertxOptions();
        options.setWorkerPoolSize(poolProperties.getWorkerPool());
        options.setEventLoopPoolSize(poolProperties.getEventLoopPool());
        options.setPreferNativeTransport(true);
        options.setWarningExceptionTimeUnit(TimeUnit.SECONDS);
        options.setWarningExceptionTime(coreProperties.getBlockingStacktraceTime());
        Vertx vertx = Vertx.vertx(options);
        configureInterceptors(vertx);
        return vertx;
    }

    /**
     * Centrally sets all verticals strictly after up all configurations. In contrast to the
     * initMethod call, it guarantees the order of the vertical deployment, since the @PostConstruct phase is executed only within the
     * configuration.
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        val coreInitializationService = event.getApplicationContext().getBean(CoreInitializationService.class);
        coreInitializationService.execute()
                .onSuccess(success -> log.debug("Dtm started successfully"))
                .onFailure(err -> {
                    log.error("Core startup error: ", err);
                    val exitCode = SpringApplication.exit(event.getApplicationContext(), () -> 1);
                    System.exit(exitCode);
                });
    }

    private void configureInterceptors(Vertx vertx) {
        vertx.eventBus().addOutboundInterceptor(event -> {
            String requestId = ContextualData.get("requestId");
            if (requestId != null) {
                event.message().headers().add("requestId", requestId);
            }
            event.next();
        });

        vertx.eventBus().addInboundInterceptor(event -> {
            String requestId = event.message().headers().get("requestId");
            if (requestId != null) {
                LoggerContextUtils.setRequestId(requestId);
            }
            event.next();
        });
    }

}
