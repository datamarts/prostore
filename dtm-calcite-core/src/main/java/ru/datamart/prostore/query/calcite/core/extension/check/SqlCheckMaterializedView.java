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
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

public class SqlCheckMaterializedView extends SqlCheckCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CHECK_MATERIALIZED_VIEW", SqlKind.CHECK);
    private final String materializedView;
    private final String schema;

    public SqlCheckMaterializedView(SqlParserPos pos, SqlIdentifier id) {
        super(pos, id);

        if (id != null) {
            val nameWithSchema = id.toString();
            this.schema = CalciteUtil.parseSchemaName(nameWithSchema);
            this.materializedView = CalciteUtil.parseTableName(nameWithSchema);
        } else {
            this.schema = null;
            this.materializedView = null;
        }
    }

    public String getMaterializedView() {
        return materializedView;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public CheckType getType() {
        return CheckType.MATERIALIZED_VIEW;
    }
}
