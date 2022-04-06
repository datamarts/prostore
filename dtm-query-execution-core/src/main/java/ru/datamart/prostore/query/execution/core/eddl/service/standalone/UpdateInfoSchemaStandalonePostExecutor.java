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
package ru.datamart.prostore.query.execution.core.eddl.service.standalone;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.eddl.dto.CreateStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.DropStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlQuery;
import ru.datamart.prostore.query.execution.plugin.api.service.PostExecutor;

@Service
public class UpdateInfoSchemaStandalonePostExecutor implements PostExecutor<EddlQuery> {

    private final InformationSchemaService informationSchemaService;

    @Autowired
    public UpdateInfoSchemaStandalonePostExecutor(InformationSchemaService informationSchemaService) {
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<Void> execute(EddlQuery eddlQuery) {
        Entity entity;
        switch (eddlQuery.getAction()) {
            case CREATE_READABLE_EXTERNAL_TABLE:
            case CREATE_WRITEABLE_EXTERNAL_TABLE:
                entity = ((CreateStandaloneExternalTableQuery) eddlQuery).getEntity();
                return informationSchemaService.update(entity, null, SqlKind.CREATE_TABLE);
            case DROP_READABLE_EXTERNAL_TABLE:
            case DROP_WRITEABLE_EXTERNAL_TABLE:
                val dropQuery = (DropStandaloneExternalTableQuery) eddlQuery;
                entity = Entity.builder()
                        .entityType(eddlQuery.getAction() == EddlAction.DROP_READABLE_EXTERNAL_TABLE ? EntityType.READABLE_EXTERNAL_TABLE : EntityType.WRITEABLE_EXTERNAL_TABLE)
                        .schema(dropQuery.getSchemaName())
                        .name(dropQuery.getTableName())
                        .build();
                return informationSchemaService.update(entity, null, SqlKind.DROP_TABLE);
            default:
                return Future.failedFuture(new DtmException(String.format("EddlQuery not supported: %s", eddlQuery.getAction())));
        }
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.UPDATE_INFORMATION_SCHEMA;
    }
}