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
package io.arenadata.dtm.query.execution.core.ddl.service.impl.post;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.query.execution.core.base.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.PostExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UpdateInfoSchemaDdlPostExecutor implements PostExecutor<DdlRequestContext> {

    private final InformationSchemaService informationSchemaService;

    @Autowired
    public UpdateInfoSchemaDdlPostExecutor(InformationSchemaService informationSchemaService) {
        this.informationSchemaService = informationSchemaService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        return informationSchemaService.update(context.getEntity(), context.getDatamartName(), context.getSqlNode().getKind());
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.UPDATE_INFORMATION_SCHEMA;
    }
}
