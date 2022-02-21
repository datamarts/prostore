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

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.execution.core.metrics.configuration.MetricsProperties;
import ru.datamart.prostore.query.execution.core.metrics.dto.MetricsSettingsUpdateResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MetricsManagementService {

    private final MetricsProvider metricsProvider;
    private final MetricsProperties metricsProperties;

    @Autowired
    public MetricsManagementService(MetricsProvider metricsProvider,
                                    MetricsProperties metricsProperties) {
        this.metricsProvider = metricsProvider;
        this.metricsProperties = metricsProperties;
    }

    public MetricsSettingsUpdateResult turnOnMetrics() {
        try {
            if (metricsProperties.isEnabled()) {
                return new MetricsSettingsUpdateResult(true, "Metrics is already turned on");
            } else {
                metricsProvider.clear();
                metricsProperties.setEnabled(true);
                final String turnedOnMsg = "Metrics have been turned on";
                log.info(turnedOnMsg);
                return new MetricsSettingsUpdateResult(true, turnedOnMsg);
            }
        } catch (Exception e) {
            throw new DtmException("Error in turning on metrics", e);
        }
    }

    public MetricsSettingsUpdateResult turnOffMetrics() {
        try {
            if (!metricsProperties.isEnabled()) {
                return new MetricsSettingsUpdateResult(false, "Metrics is already turned off");
            } else {
                metricsProperties.setEnabled(false);
                final String turnedOffMsg = "Metrics have been turned off";
                log.info(turnedOffMsg);
                return new MetricsSettingsUpdateResult(false, turnedOffMsg);
            }
        } catch (Exception e) {
            throw new DtmException("Error in turning off metrics", e);
        }
    }
}
