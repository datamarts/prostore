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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.dto.schema.DatamartSchemaKey;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class LogicalSchemaProvider {

    private final LogicalSchemaService logicalSchemaService;

    @Autowired
    public LogicalSchemaProvider(LogicalSchemaService logicalSchemaService) {
        this.logicalSchemaService = logicalSchemaService;
    }

    public Future<List<Datamart>> getSchemaFromQuery(SqlNode query, String datamart) {
        return logicalSchemaService.createSchemaFromQuery(query, datamart)
                .map(schemaMap -> {
                    log.trace("Received data schema on request: {}; {}", query, schemaMap);
                    return getDatamartsSchemas(datamart, schemaMap);
                });
    }

    public Future<List<Datamart>> getSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations, String datamart) {
        return logicalSchemaService.createSchemaFromDeltaInformations(deltaInformations)
                .map(schemaMap -> {
                    log.trace("Received data schema on delta information: {}; {}", deltaInformations, schemaMap);
                    return getDatamartsSchemas(datamart, schemaMap);
                });
    }

    @NotNull
    private List<Datamart> getDatamartsSchemas(String defaultDatamart,
                                               Map<DatamartSchemaKey, Entity> datamartSchemaMap) {
        Map<String, Datamart> datamartMap = new HashMap<>();
        datamartSchemaMap.forEach((k, v) -> {
            final Datamart datamart = createDatamart(k.getSchema());
            if (datamart.getMnemonic().equals(defaultDatamart)) {
                datamart.setIsDefault(true);
            }
            datamartMap.putIfAbsent(k.getSchema(), datamart);
            datamartMap.get(k.getSchema()).getEntities().add(v);
        });
        return new ArrayList<>(datamartMap.values());
    }

    @NotNull
    private Datamart createDatamart(String schema) {
        Datamart datamart = new Datamart();
        datamart.setMnemonic(schema);
        datamart.setEntities(new ArrayList<>());
        return datamart;
    }

}
