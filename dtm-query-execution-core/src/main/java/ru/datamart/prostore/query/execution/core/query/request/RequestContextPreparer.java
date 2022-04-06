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
package ru.datamart.prostore.query.execution.core.query.request;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;
import ru.datamart.prostore.query.execution.core.query.request.specific.SpecificRequestContextPreparer;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class RequestContextPreparer {
    private final List<SpecificRequestContextPreparer> requestContextPreparers;

    public RequestContextPreparer(List<SpecificRequestContextPreparer> requestContextPreparers) {
        this.requestContextPreparers = requestContextPreparers.stream()
                .sorted(Comparator.comparingInt(SpecificRequestContextPreparer::priority).reversed())
                .collect(Collectors.toList());
    }

    public Future<CoreRequestContext<? extends DatamartRequest, ? extends SqlNode>> prepare(QueryRequest request, SqlNode node) {
        return Future.future(promise -> {
            for (val requestContextPreparer : requestContextPreparers) {
                if (requestContextPreparer.isApplicable(node)) {
                    requestContextPreparer.create(request, node)
                            .onComplete(promise);
                    return;
                }
            }

            //should be unreachable
            promise.fail(new DtmException(String.format("Could not prepare request context, no suitable RequestContextPreparer for [%s]", request.getSql())));
        });
    }
}
