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
package io.arenadata.dtm.query.execution.plugin.api.service.mppw;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MppwServiceImpl<T extends MppwExecutor> implements MppwService {
    private final Map<ExternalTableLocationType, MppwExecutor> executors;

    public MppwServiceImpl(List<T> executors) {
        this.executors = executors.stream()
                .collect(Collectors.toMap(MppwExecutor::getType, Function.identity()));
    }

    @Override
    public Future<String> execute(MppwRequest request) {
        MppwExecutor mppwExecutor = executors.get(request.getExternalTableLocationType());
        if (mppwExecutor == null) {
            return Future.failedFuture(
                    new DtmException("Not implemented mppw external table location: " + request.getExternalTableLocationType()));
        }

        return mppwExecutor.execute(request);
    }
}
