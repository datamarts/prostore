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
package ru.datamart.prostore.calcite.adqm.extension.dml;

import ru.datamart.prostore.query.calcite.core.extension.ddl.SingleDatasourceOperator;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlEstimateOnlyQuery;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.List;

@Getter
@Setter
public class LimitableSqlOrderBy extends SqlOrderBy implements SqlDataSourceTypeGetter, SqlEstimateOnlyQuery {
    private static final SqlSpecialOperator OPERATOR = new LimitableSqlOrderBy.Operator() {
        @Override
        public SqlCall createCall(SqlLiteral functionQualifier,
                                  SqlParserPos pos, SqlNode... operands) {
            return new LimitableSqlOrderBy(pos, operands[0], (SqlNodeList) operands[1],
                    operands[2], operands[3], null, false);
        }
    };
    private SqlKind kind;
    private SingleDatasourceOperator datasourceType;
    private boolean estimate;

    public LimitableSqlOrderBy(SqlParserPos pos,
                               SqlNode query,
                               SqlNodeList orderList,
                               SqlNode fetch,
                               SqlNode offset,
                               SqlNode datasourceType,
                               boolean estimate) {
        super(pos, query, orderList, offset, fetch);
        kind = SqlKind.ORDER_BY;
        this.datasourceType = new SingleDatasourceOperator(pos, datasourceType);
        this.estimate = estimate;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(query,
                orderList,
                fetch,
                offset);
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public SqlKind getKind() {
        return kind;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new LimitableSqlOrderBy(
                pos,
                query,
                orderList,
                fetch,
                offset,
                datasourceType.getOriginalNode(),
                estimate
        );
    }

    @Override
    @SneakyThrows
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                setOperand(operand, "query");
                break;
            case 1:
                setOperand(operand, "orderList");
                break;
            case 2:
                setOperand(operand, "fetch");
                break;
            case 3:
                setOperand(operand, "offset");
                break;
            default:
                break;
        }
    }

    private void setOperand(Object operand, String query) throws IllegalAccessException {
        writeField(this, query, operand);
    }

    private void writeField(Object target, String fieldName, Object value) throws IllegalAccessException {
        Validate.notNull(target, "target object must not be null");
        Class<?> cls = target.getClass();
        Field field = FieldUtils.getField(cls, fieldName, true);
        Validate.isTrue(field != null, "Cannot locate declared field %s.%s", cls.getName(), fieldName);
        FieldUtils.writeField(field, target, value, true);
    }

    /**
     * Definition of {@code ORDER BY} operator.
     */
    private static class Operator extends SqlSpecialOperator {
        private Operator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super("ORDER BY", SqlKind.ORDER_BY, 0);
        }

        @Override
        public SqlSyntax getSyntax() {
            return SqlSyntax.POSTFIX;
        }

        @Override
        public void unparse(
                SqlWriter writer,
                SqlCall call,
                int leftPrec,
                int rightPrec) {
            LimitableSqlOrderBy orderBy = (LimitableSqlOrderBy) call;
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
            orderBy.query.unparse(writer, getLeftPrec(), getRightPrec());
            if (!orderBy.orderList.equalsDeep(SqlNodeList.EMPTY, Litmus.IGNORE)) {
                writer.sep(getName());
                writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                        orderBy.orderList);
            }

            if (orderBy.fetch != null) {
                final SqlWriter.Frame frame2 =
                        writer.startList(SqlWriter.FrameTypeEnum.FETCH);
                writer.newlineAndIndent();
                writer.keyword("LIMIT");
                orderBy.fetch.unparse(writer, -1, -1);
                writer.endList(frame2);
            }

            if (orderBy.offset != null) {
                final SqlWriter.Frame frame3 =
                        writer.startList(SqlWriter.FrameTypeEnum.OFFSET);
                writer.newlineAndIndent();
                writer.keyword("OFFSET");
                orderBy.offset.unparse(writer, -1, -1);
                writer.endList(frame3);
            }
            writer.endList(frame);
        }
    }
}

