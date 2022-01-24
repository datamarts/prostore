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
package io.arenadata.dtm.query.execution.core.metrics.repository;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Repository("mapActiveRequestsRepository")
public class ActiveRequestsRepository {

    private final Map<UUID, RequestMetrics> requestsMap = new ConcurrentHashMap<>();

    public void add(RequestMetrics request) {
        requestsMap.put(request.getRequestId(), request);
    }

    public void remove(RequestMetrics request) {
        requestsMap.remove(request.getRequestId());
    }

    public RequestMetrics get(UUID requestId) {
        return requestsMap.get(requestId);
    }

    public List<RequestMetrics> getList() {
        return new ArrayList<>(requestsMap.values());
    }

    public void deleteAll() {
        requestsMap.clear();
    }
}
