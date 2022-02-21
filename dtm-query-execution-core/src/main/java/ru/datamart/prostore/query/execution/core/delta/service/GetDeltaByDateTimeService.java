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
package ru.datamart.prostore.query.execution.core.delta.service;

import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaAction;
import ru.datamart.prostore.query.execution.core.delta.dto.query.DeltaQuery;
import ru.datamart.prostore.query.execution.core.delta.factory.DeltaQueryResultFactory;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class GetDeltaByDateTimeService extends GetDeltaOkService implements DeltaService {

    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public GetDeltaByDateTimeService(ServiceDbFacade serviceDbFacade,
                                     @Qualifier("deltaOkQueryResultFactory") DeltaQueryResultFactory deltaQueryResultFactory) {
        super(serviceDbFacade, deltaQueryResultFactory);
        this.deltaServiceDao = serviceDbFacade.getDeltaServiceDao();
    }

    @Override
    public Future<QueryResult> execute(DeltaQuery deltaQuery) {
        return deltaServiceDao.getDeltaByDateTime(deltaQuery.getDatamart(), deltaQuery.getDeltaDate())
                .map(deltaOk -> createResult(deltaOk, deltaQuery));
    }

    @Override
    public DeltaAction getAction() {
        return DeltaAction.GET_DELTA_BY_DATETIME;
    }
}
