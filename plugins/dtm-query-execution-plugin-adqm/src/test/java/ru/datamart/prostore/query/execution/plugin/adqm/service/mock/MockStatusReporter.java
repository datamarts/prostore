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
package ru.datamart.prostore.query.execution.plugin.adqm.service.mock;

import ru.datamart.prostore.query.execution.plugin.adqm.status.dto.StatusReportDto;
import ru.datamart.prostore.query.execution.plugin.adqm.status.service.StatusReporter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MockStatusReporter implements StatusReporter {
    private final Map<String, StatusReportDto> expectedPayloads;
    private final Set<String> calls = new HashSet<>();

    public MockStatusReporter(@NonNull final Map<String, StatusReportDto> expectedPayloads) {
        this.expectedPayloads = new HashMap<>(expectedPayloads);
    }

    @Override
    public void onStart(StatusReportDto payload) {
        calls.add("start");
        StatusReportDto expectedPayload = expectedPayloads.get("start");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onFinish(StatusReportDto payload) {
        calls.add("finish");
        StatusReportDto expectedPayload = expectedPayloads.get("finish");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onError(StatusReportDto payload) {
        calls.add("error");
        StatusReportDto expectedPayload = expectedPayloads.get("error");
        assertEquals(expectedPayload, payload);
    }

    public boolean wasCalled(String action) {
        return calls.contains(action);
    }
}
