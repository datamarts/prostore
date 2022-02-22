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
package ru.datamart.prostore.query.execution.core.metrics.service;

import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.metrics.MetricsTopic;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.metrics.configuration.MetricsProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service("coreMetricsService")
public class MetricsService {

    private final MetricsProducer metricsProducer;
    private final MetricsProperties metricsProperties;

    @Autowired
    public MetricsService(MetricsProducer metricsProducer, MetricsProperties metricsProperties) {
        this.metricsProducer = metricsProducer;
        this.metricsProperties = metricsProperties;
    }

    public <R> Handler<AsyncResult<R>> sendMetrics(SourceType type,
                                                   SqlProcessingType actionType,
                                                   RequestMetrics requestMetrics,
                                                   Handler<AsyncResult<R>> handler) {
        if (!metricsProperties.isEnabled()) {
            return ar -> {
                if (ar.succeeded()) {
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            };
        } else {
            return ar -> {
                updateMetrics(type, actionType, requestMetrics);
                if (ar.succeeded()) {
                    requestMetrics.setStatus(RequestStatus.SUCCESS);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handle(Future.succeededFuture(ar.result()));
                } else {
                    requestMetrics.setStatus(RequestStatus.ERROR);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            };
        }

    }

    public Future<Void> sendMetrics(SourceType type,
                                    SqlProcessingType actionType,
                                    RequestMetrics requestMetrics) {
        if (!metricsProperties.isEnabled()) {
            return Future.succeededFuture();
        } else {
            return Future.future(promise -> {
                requestMetrics.setSourceType(type);
                requestMetrics.setActionType(actionType);
                metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                promise.complete();
            });
        }
    }

    private void updateMetrics(SourceType type, SqlProcessingType actionType, RequestMetrics metrics) {
        metrics.setActive(false);
        metrics.setEndTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID));
        metrics.setSourceType(type);
        metrics.setActionType(actionType);
    }
}
