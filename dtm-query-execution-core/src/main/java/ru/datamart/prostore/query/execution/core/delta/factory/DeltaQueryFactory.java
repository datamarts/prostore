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
package ru.datamart.prostore.query.execution.core.delta.factory;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.extension.delta.*;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaByNum;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaHot;
import ru.datamart.prostore.query.calcite.core.extension.delta.function.SqlGetDeltaOk;
import ru.datamart.prostore.query.execution.core.delta.dto.operation.DeltaRequestContext;
import ru.datamart.prostore.query.execution.core.delta.dto.query.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;
import static ru.datamart.prostore.query.execution.core.delta.utils.DeltaQueryUtil.DELTA_DATE_TIME_PATTERN;

@Component
@Slf4j
public class DeltaQueryFactory {

    private final SqlDialect sqlDialect;

    @Autowired
    public DeltaQueryFactory(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public DeltaQuery create(DeltaRequestContext context) {
        val sqlNode = context.getSqlNode();
        if (context.getSqlNode() instanceof SqlBeginDelta) {
            return BeginDeltaQuery.builder()
                    .deltaNum(((SqlBeginDelta) sqlNode).getDeltaNumOperator().getNum())
                    .build();
        } else if (sqlNode instanceof SqlCommitDelta) {
            return CommitDeltaQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlCommitDelta) sqlNode).getDeltaDateTimeOperator().getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaOk) {
            return GetDeltaOkQuery.builder()
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaHot) {
            return GetDeltaHotQuery.builder()
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaByNum) {
            return GetDeltaByNumQuery.builder()
                    .deltaNum(((SqlGetDeltaByNum) sqlNode).getDeltaNum())
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaByDateTime) {
            return GetDeltaByDateTimeQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlGetDeltaByDateTime) sqlNode).getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlRollbackDelta) {
            return RollbackDeltaQuery.builder()
                    .sqlNode((SqlRollbackDelta) sqlNode)
                    .envName(context.getEnvName())
                    .build();
        } else if (sqlNode instanceof ResumeWriteOperation) {
            return ResumeWriteOperationDeltaQuery.builder()
                    .sysCn(((ResumeWriteOperation) sqlNode).getWriteOperationNumber())
                    .build();
        } else if (sqlNode instanceof GetWriteOperations) {
            return GetWriteOperationsDeltaQuery.builder().build();
        } else {
            throw new DtmException(String.format("Query [%s] is not a DELTA operator",
                    sqlNode.toSqlString(sqlDialect)));
        }
    }

    private LocalDateTime getDeltaDateTime(String deltaDateTimeStr) {
        if (deltaDateTimeStr != null) {
            try {
                return LocalDateTime.parse(deltaDateTimeStr, DELTA_DATE_TIME_FORMATTER);
            } catch (Exception e) {
                throw new DtmException(String.format("Incorrect format of delta date value: %s, correct template: %s",
                        deltaDateTimeStr, DELTA_DATE_TIME_PATTERN), e);
            }
        } else {
            return null;
        }
    }
}
