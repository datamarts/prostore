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
package ru.datamart.prostore.query.calcite.core.service.impl;

import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public abstract class CalciteDefinitionService implements DefinitionService<SqlNode> {
    private final SqlParser.Config config;

    public CalciteDefinitionService(SqlParser.Config config) {
        this.config = config;
    }

    @SneakyThrows
    public SqlNode processingQuery(String sql) {
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseQuery();
    }
}
