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
package ru.datamart.prostore.query.execution.core.metrics.service;

import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.query.execution.core.metrics.repository.ActiveRequestsRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

import static ru.datamart.prostore.query.execution.core.metrics.utils.MetricsUtil.*;

@Service
@Slf4j
public class MetricsProcessingService {

    private final MeterRegistry meterRegistry;
    private final ActiveRequestsRepository activeRequestsRepository;

    @Autowired
    public MetricsProcessingService(MeterRegistry meterRegistry,
                                    @Qualifier("mapActiveRequestsRepository") ActiveRequestsRepository activeRequestsRepository) {
        this.meterRegistry = meterRegistry;
        this.activeRequestsRepository = activeRequestsRepository;
    }

    public void process(RequestMetrics metricsValue) {
        log.debug("Received metrics request: {}", metricsValue);
        if (metricsValue.isActive()) {
            this.activeRequestsRepository.add(metricsValue);
        } else {
            this.activeRequestsRepository.remove(metricsValue);
            getRequestsCounter(metricsValue, REQUESTS_AMOUNT).increment();
            getRequestsTimer(metricsValue, REQUESTS_TIME)
                    .record(Duration.between(metricsValue.getStartTime(),
                            metricsValue.getEndTime()));
        }
    }

    private Timer getRequestsTimer(RequestMetrics metricsValue, String requestsTimeName) {
        return Objects.requireNonNull(meterRegistry
                .find(requestsTimeName)
                .tags(ACTION_TYPE, metricsValue.getActionType().name(),
                        SOURCE_TYPE, metricsValue.getSourceType().name())
                .timer());
    }

    private Counter getRequestsCounter(RequestMetrics metricsValue, String requestsCounterName) {
        return Objects.requireNonNull(meterRegistry
                .find(requestsCounterName)
                .tags(ACTION_TYPE, metricsValue.getActionType().name(),
                        SOURCE_TYPE, metricsValue.getSourceType().name())
                .counter());
    }
}
