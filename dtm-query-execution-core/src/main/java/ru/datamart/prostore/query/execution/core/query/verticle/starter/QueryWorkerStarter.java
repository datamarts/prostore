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
package ru.datamart.prostore.query.execution.core.query.verticle.starter;

import ru.datamart.prostore.query.execution.core.base.configuration.properties.CoreHttpProperties;
import ru.datamart.prostore.query.execution.core.query.controller.DatamartMetaController;
import ru.datamart.prostore.query.execution.core.query.verticle.QueryVerticle;
import ru.datamart.prostore.query.execution.core.metrics.controller.MetricsController;
import ru.datamart.prostore.query.execution.core.query.controller.QueryController;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueryWorkerStarter {
    private final DatamartMetaController datamartMetaController;
    private final MetricsController metricsController;
    private final CoreHttpProperties httpProperties;
    private final QueryController queryController;

    public Future<Void> start(Vertx vertx) {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        return Future.future(p -> vertx.deployVerticle(() -> new QueryVerticle(httpProperties, datamartMetaController, queryController, metricsController),
                new DeploymentOptions().setInstances(availableProcessors),
                ar -> {
                    if (ar.succeeded()) {
                        log.debug("Verticles '{}'({}) deployed successfully", QueryVerticle.class.getSimpleName(), availableProcessors);
                        log.info("The server is running on the port: {}", httpProperties.getPort());
                        p.complete();
                    } else {
                        log.error("Verticles deploy error", ar.cause());
                        p.fail(ar.cause());
                    }
                }));
    }

}
