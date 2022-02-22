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
package ru.datamart.prostore.query.execution.core.metrics.controller.endpoint;

import ru.datamart.prostore.query.execution.core.metrics.dto.ResultMetrics;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.stereotype.Component;

@WebEndpoint(id = "requests")
@Component
public class RequestEndpoint {

    private final MetricsProvider metricsProvider;

    @Autowired
    public RequestEndpoint(MetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    @ReadOperation
    public ResultMetrics metrics() {
        return metricsProvider.get();
    }
}