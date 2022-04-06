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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.select.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.api.exception.FeatureNotImplementedException;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.InsertSelectService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("adgInsertSelectService")
public class AdgInsertSelectService implements InsertSelectService {
    private final Map<SourceType, DestinationInsertSelectHandler> logicalHandlers;
    private final Map<SourceType, DestinationInsertSelectHandler> standaloneHandlers;

    public AdgInsertSelectService(List<DestinationInsertSelectHandler> handlers) {
        this.logicalHandlers = handlers.stream()
                .filter(DestinationInsertSelectHandler::isLogical)
                .collect(Collectors.toMap(DestinationInsertSelectHandler::getDestinations, handler -> handler));
        this.standaloneHandlers = handlers.stream()
                .filter(handler -> !handler.isLogical())
                .collect(Collectors.toMap(DestinationInsertSelectHandler::getDestinations, handler -> handler));
    }

    @Override
    public Future<Void> execute(InsertSelectRequest request) {
        List<Future> futures = new ArrayList<>();
        val handlers = request.getEntity().getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE
                ? standaloneHandlers
                : logicalHandlers;
        for (SourceType sourceType : request.getEntity().getDestination()) {
            val handler = handlers.get(sourceType);
            if (handler == null) {
                futures.add(Future.failedFuture(new FeatureNotImplementedException(String.format("insert select from [ADG] to [%s]", sourceType))));
                break;
            }

            futures.add(handler.handle(request));
        }

        return CompositeFuture.join(futures)
                .mapEmpty();
    }
}
