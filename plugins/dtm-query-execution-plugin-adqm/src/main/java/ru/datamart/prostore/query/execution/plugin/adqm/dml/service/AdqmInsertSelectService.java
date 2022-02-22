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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insert.DestinationInsertSelectHandler;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertSelectRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.InsertSelectService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("adqmInsertSelectService")
public class AdqmInsertSelectService implements InsertSelectService {
    private final Map<SourceType, DestinationInsertSelectHandler> handlers;

    public AdqmInsertSelectService(List<DestinationInsertSelectHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(DestinationInsertSelectHandler::getDestinations, handler -> handler));
    }

    @Override
    public Future<Void> execute(InsertSelectRequest request) {
        List<Future> futures = new ArrayList<>();
        for (SourceType sourceType : request.getEntity().getDestination()) {
            val handler = handlers.get(sourceType);
            if (handler == null) {
                futures.add(Future.failedFuture(new DtmException(String.format("Insert select from [ADQM] to [%s] is not implemented", sourceType))));
                break;
            }

            futures.add(handler.handle(request));
        }

        return CompositeFuture.join(futures)
                .mapEmpty();
    }
}
