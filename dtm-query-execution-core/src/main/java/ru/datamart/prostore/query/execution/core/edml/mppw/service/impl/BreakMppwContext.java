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
package ru.datamart.prostore.query.execution.core.edml.mppw.service.impl;

import ru.datamart.prostore.query.execution.core.delta.dto.request.BreakMppwRequest;
import ru.datamart.prostore.query.execution.core.edml.mppw.dto.MppwStopReason;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BreakMppwContext {

    private static final Map<BreakMppwRequest, MppwStopReason> breakRequests = new ConcurrentHashMap<>();

    public static void requestRollback(String datamart, long sysCn, MppwStopReason reason) {
        breakRequests.put(new BreakMppwRequest(datamart, sysCn), reason);
    }

    public static boolean rollbackRequested(String datamart, long sysCn) {
        return breakRequests.containsKey(new BreakMppwRequest(datamart, sysCn));
    }

    public static void removeTask(String datamart, long sysCn) {
        breakRequests.remove(new BreakMppwRequest(datamart, sysCn));
    }

    public static MppwStopReason getReason(String datamart, long sysCn) {
        return breakRequests.get(new BreakMppwRequest(datamart, sysCn));
    }

    public static long getNumberOfTasksByDatamart(String datamart) {
        return breakRequests.keySet().stream().filter(task -> task.getDatamart().equals(datamart)).count();
    }

}
