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
package ru.datamart.prostore.query.execution.core.dml.dto;

import lombok.*;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.dml.DmlType;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlUseSchema;
import ru.datamart.prostore.query.execution.core.base.dto.request.CoreRequestContext;

import static ru.datamart.prostore.common.model.SqlProcessingType.DML;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.isSelectSqlNode;

@Getter
@Setter
@ToString
public class DmlRequestContext extends CoreRequestContext<DmlRequest, SqlNode> {

    private final SourceType sourceType;
    private final DmlType type;

    @Builder
    protected DmlRequestContext(RequestMetrics metrics,
                                String envName,
                                DmlRequest request,
                                SqlNode sqlNode,
                                SourceType sourceType) {
        super(metrics, envName, request, sqlNode);
        this.sourceType = sourceType;
        type = calculateType(sqlNode);
    }

    private DmlType calculateType(SqlNode sqlNode) {
        if (sqlNode instanceof SqlUseSchema) {
            return DmlType.USE;
        }

        if (sqlNode instanceof SqlInsert) {
            val sqlInsert = (SqlInsert) sqlNode;
            if (isSelectSqlNode(((SqlInsert) sqlNode).getSource())) {
                return sqlInsert.isUpsert() ? DmlType.UPSERT_SELECT : DmlType.INSERT_SELECT;
            } else {
                return sqlInsert.isUpsert() ? DmlType.UPSERT_VALUES : DmlType.INSERT_VALUES;
            }
        }

        if (sqlNode instanceof SqlDelete) {
            return DmlType.DELETE;
        }

        return DmlType.LLR;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return DML;
    }
}
