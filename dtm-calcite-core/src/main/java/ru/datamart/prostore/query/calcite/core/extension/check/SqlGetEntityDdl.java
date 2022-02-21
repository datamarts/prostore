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
package ru.datamart.prostore.query.calcite.core.extension.check;

import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import lombok.Getter;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

@Getter
public class SqlGetEntityDdl extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("GET_ENTITY_DDL", SqlKind.CHECK);
    private final String schema;
    private final String entity;

    public SqlGetEntityDdl(SqlParserPos pos,
                           SqlIdentifier id) {
        super(pos, id);
        if (id == null) {
            throw new IllegalArgumentException("Entity name is not specified");
        }
        val nameWithSchema = id.toString();
        this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
        this.entity = CalciteUtil.parseTableName(nameWithSchema);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.ENTITY_DDL;
    }

}
