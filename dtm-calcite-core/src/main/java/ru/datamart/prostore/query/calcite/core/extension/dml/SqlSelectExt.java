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
package ru.datamart.prostore.query.calcite.core.extension.dml;

import ru.datamart.prostore.query.calcite.core.extension.ddl.SingleDatasourceOperator;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlSelectExt extends SqlSelect implements SqlDataSourceTypeGetter, SqlEstimateOnlyQuery {

    private SingleDatasourceOperator datasourceType;
    private boolean estimate;

    public SqlSelectExt(SqlParserPos pos,
                        SqlNodeList keywordList,
                        SqlNodeList selectList,
                        SqlNode from,
                        SqlNode where,
                        SqlNodeList groupBy,
                        SqlNode having,
                        SqlNodeList windowDecls,
                        SqlNodeList orderBy,
                        SqlNode offset,
                        SqlNode fetch,
                        SqlNodeList hints,
                        SqlNode datasourceType,
                        boolean estimate) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
        this.datasourceType = new SingleDatasourceOperator(pos, datasourceType);
        this.estimate = estimate;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        datasourceType.unparse(writer, leftPrec, rightPrec);
    }
}
