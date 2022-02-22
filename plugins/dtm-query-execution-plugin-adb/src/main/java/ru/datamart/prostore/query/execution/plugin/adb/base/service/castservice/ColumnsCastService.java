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
package ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface ColumnsCastService {
    default Future<SqlNode> apply(QueryParserResponse parserResponse, List<ColumnType> expectedTypes) {
        return apply(parserResponse.getSqlNode(), parserResponse.getRelNode().rel, expectedTypes);
    }

    Future<SqlNode> apply(SqlNode sqlNode, RelNode relNode, List<ColumnType> expectedTypes);
}
