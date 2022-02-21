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
package ru.datamart.prostore.query.execution.core.dml.service;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.InformationSchemaView;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class InformationSchemaDefinitionService {
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String LOGIC_PREFIX = "logic_";

    public boolean isInformationSchemaRequest(List<DeltaInformation> deltaInformations) {
        Set<String> unicSchemes = deltaInformations.stream()
                .map(DeltaInformation::getSchemaName)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        boolean informationSchemaExists = unicSchemes.contains(InformationSchemaView.SCHEMA_NAME);

        if (unicSchemes.size() > 1 && informationSchemaExists) {
            throw new DtmException("Simultaneous query to the information schema and user schema isn't supported");
        } else {
            return informationSchemaExists;
        }
    }

    public Future<Void> checkAccessToSystemLogicalTables(SqlNode query) {
        return Future.future(p -> {
            boolean existsSystemLogicTable = new SqlSelectTree(query).findAllTableAndSnapshots().stream()
                    .filter(tableNode -> INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.tryGetSchemaName().orElse("")))
                    .map(tableNode -> tableNode.tryGetTableName().orElseThrow(() -> new DtmException("Can't get table name")))
                    .anyMatch(tableName -> tableName.toLowerCase().startsWith(LOGIC_PREFIX));
            if (existsSystemLogicTable) {
                p.fail(new DtmException("Access to system logical tables is forbidden"));
            } else {
                p.complete();
            }
        });
    }
}
