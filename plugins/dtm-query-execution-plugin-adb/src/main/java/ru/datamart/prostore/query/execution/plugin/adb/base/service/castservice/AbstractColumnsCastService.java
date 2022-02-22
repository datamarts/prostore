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

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import io.vertx.core.Future;
import lombok.val;
import lombok.var;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public abstract class AbstractColumnsCastService implements ColumnsCastService {
    private static final SqlPredicates COLUMN_SELECT = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqFromStart(SqlKind.SELECT))
            .anyOf(SqlPredicatePart.eq(SqlKind.OTHER))
            .build();
    private static final SqlIntervalQualifier EPOCH = new SqlIntervalQualifier(TimeUnit.EPOCH, TimeUnit.EPOCH, SqlParserPos.ZERO);
    private static final SqlDataTypeSpec BIGINT_TYPE = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO), SqlParserPos.ZERO);
    private static final SqlDataTypeSpec DATE_TYPE = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.DATE, SqlParserPos.ZERO), SqlParserPos.ZERO);
    private static final SqlDataTypeSpec TIMESTAMP_TYPE = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, SqlParserPos.ZERO), SqlParserPos.ZERO);
    private static final SqlDataTypeSpec TIME_TYPE = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIME, SqlParserPos.ZERO), SqlParserPos.ZERO);
    private static final SqlLiteral ONE_DAY_IN_SECONDS = SqlLiteral.createExactNumeric("86400", SqlParserPos.ZERO);
    private static final SqlLiteral MICROSECONDS_IN_SECOND = SqlLiteral.createExactNumeric("1000000", SqlParserPos.ZERO);

    private final SqlDialect sqlDialect;

    protected AbstractColumnsCastService(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<SqlNode> apply(SqlNode sqlNode, RelNode relNode, List<ColumnType> expectedTypes) {
        return Future.future(promise -> {
            val sqlNodeTree = new SqlSelectTree(sqlNode);
            val columnsNode = sqlNodeTree.findNodes(COLUMN_SELECT, true);
            if (columnsNode.size() != 1) {
                throw new DtmException(format("Expected one node contain columns: %s", sqlNode.toSqlString(sqlDialect).toString()));
            }

            val columnsNodes = sqlNodeTree.findNodesByParent(columnsNode.get(0));
            val columnsTypes = relNode
                    .getRowType()
                    .getFieldList()
                    .stream()
                    .map(RelDataTypeField::getType)
                    .map(RelDataType::getSqlTypeName)
                    .collect(Collectors.toList());

            for (int i = 0; i < columnsNodes.size(); i++) {
                val columnSqlType = columnsTypes.get(i);
                val columnLogicalType = expectedTypes.get(i);

                if (!isCastType(columnLogicalType)) {
                    continue;
                }

                var columnNode = columnsNodes.get(i);
                if (columnNode.getNode().getKind() == SqlKind.AS) {
                    columnNode = sqlNodeTree.findNodesByParent(columnNode).get(0);
                }

                columnNode.getSqlNodeSetter().accept(surroundWith(columnLogicalType, columnSqlType, columnNode.getNode()));
            }

            promise.complete(sqlNode);
        });
    }

    protected abstract SqlNode surroundWith(ColumnType logicalType, SqlTypeName sqlType, SqlNode node);

    protected abstract boolean isCastType(ColumnType columnLogicalType);

    protected SqlNode surroundDateNode(SqlNode sqlNode, SqlTypeName sqlType) {
        var sqlNodeToSurround = sqlNode;
        if (sqlType != SqlTypeName.DATE) {
            sqlNodeToSurround = SqlNodeTemplates.basicCall(SqlStdOperatorTable.CAST, sqlNodeToSurround, DATE_TYPE);
        }

        val epoch = getEpoch(sqlNodeToSurround);
        val convertToDays = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[]{epoch, ONE_DAY_IN_SECONDS}, SqlParserPos.ZERO);
        return castToBigint(convertToDays);
    }

    protected SqlNode surroundTimestampNode(SqlNode sqlNode, SqlTypeName sqlType) {
        var sqlNodeToSurround = sqlNode;
        if (sqlType != SqlTypeName.TIMESTAMP) {
            sqlNodeToSurround = SqlNodeTemplates.basicCall(SqlStdOperatorTable.CAST, sqlNodeToSurround, TIMESTAMP_TYPE);
        }
        val epoch = getEpoch(sqlNodeToSurround);
        val convertToMicroseconds = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, new SqlNode[]{epoch, MICROSECONDS_IN_SECOND}, SqlParserPos.ZERO);
        return castToBigint(convertToMicroseconds);
    }

    protected SqlNode surroundTimeNode(SqlNode sqlNode, SqlTypeName sqlType) {
        var sqlNodeToSurround = sqlNode;
        if (sqlType != SqlTypeName.TIME) {
            sqlNodeToSurround = SqlNodeTemplates.basicCall(SqlStdOperatorTable.CAST, sqlNodeToSurround, TIME_TYPE);
        }
        val epoch = getEpoch(sqlNodeToSurround);
        val convertToMicroseconds = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, new SqlNode[]{epoch, MICROSECONDS_IN_SECOND}, SqlParserPos.ZERO);
        return castToBigint(convertToMicroseconds);
    }

    private SqlBasicCall getEpoch(SqlNode sqlNode) {
        return new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{EPOCH, sqlNode}, SqlParserPos.ZERO);
    }

    private SqlBasicCall castToBigint(SqlBasicCall sqlNode) {
        return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[]{sqlNode, BIGINT_TYPE}, SqlParserPos.ZERO);
    }
}
