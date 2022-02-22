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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.verticle;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.handler.AdbMppwTransferDataHandler;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.verticle.worker.AdbMppwWorker;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AdbMppwVerticle extends AbstractVerticle {

    private final Map<String, MppwKafkaRequestContext> requestMap = new ConcurrentHashMap<>();
    private final Map<String, Future> resultMap = new ConcurrentHashMap<>();
    private final AdbMppwProperties mppwProperties;
    private final AdbMppwTransferDataHandler mppwTransferDataHandler;

    @Autowired
    public AdbMppwVerticle(AdbMppwProperties adbMppwProperties,
                           @Qualifier("adbMppwTransferDataHandler") AdbMppwTransferDataHandler mppwTransferDataHandler) {
        this.mppwProperties = adbMppwProperties;
        this.mppwTransferDataHandler = mppwTransferDataHandler;
    }

    @Override
    public void start() {
        val options = new DeploymentOptions()
                .setWorkerPoolSize(this.mppwProperties.getPoolSize())
                .setWorker(true);
        for (int i = 0; i < this.mppwProperties.getPoolSize(); i++) {
            vertx.deployVerticle(new AdbMppwWorker(this.requestMap, this.resultMap,
                    this.mppwTransferDataHandler), options, ar -> {
                if (ar.succeeded()) {
                    log.debug("Mppw workers deployed successfully");
                } else {
                    log.error("Error deploying mppw workers", ar.cause());
                }
            });
        }
    }
}
