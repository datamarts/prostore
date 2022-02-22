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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeSchemaGenerator;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTableExecutor implements DdlExecutor<Void> {

    private final DropTableExecutor dropTableExecutor;
    private final AdgCartridgeSchemaGenerator generator;
    private final AdgCartridgeClient client;

    @Autowired
    public CreateTableExecutor(DropTableExecutor dropTableExecutor,
                               AdgCartridgeSchemaGenerator generator,
                               AdgCartridgeClient client) {
        this.dropTableExecutor = dropTableExecutor;
        this.generator = generator;
        this.client = client;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return dropTableExecutor.execute(request)
                .compose(result -> generator.generate(request, new OperationYaml()))
                .compose(client::executeCreateSpacesQueued);
    }


    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adgDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
