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
package ru.datamart.prostore.query.calcite.core.framework;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class DtmSqlToRelConverter extends SqlToRelConverter {

    public DtmSqlToRelConverter(RelOptTable.ViewExpander viewExpander,
                                SqlValidator validator,
                                Prepare.CatalogReader catalogReader,
                                RelOptCluster cluster,
                                SqlRexConvertletTable convertletTable,
                                Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    protected void gatherOrderExprs(Blackboard bb,
                                    SqlSelect select,
                                    SqlNodeList orderList,
                                    List<SqlNode> extraOrderExprs,
                                    List<RelFieldCollation> collationList) {
        assert bb.root != null : "precondition: child != null";
        assert select != null;

        if (orderList != null) {
            for (SqlNode orderItem : orderList) {
                collationList.add(this.convertOrderItem(select,
                        orderItem,
                        extraOrderExprs,
                        RelFieldCollation.Direction.ASCENDING,
                        RelFieldCollation.NullDirection.UNSPECIFIED));
            }

        }
    }

    @Override
    protected void convertOrder(SqlSelect select,
                                Blackboard bb,
                                RelCollation collation,
                                List<SqlNode> orderExprList,
                                SqlNode offset,
                                SqlNode fetch) {
        if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
            assert collation.getFieldCollations().isEmpty();

            if ((offset == null || offset instanceof SqlLiteral
                    && ((SqlLiteral) offset).bigDecimalValue().equals(BigDecimal.ZERO))
                    && fetch == null) {
                return;
            }
        }

        bb.setRoot(LogicalSort.create(bb.root,
                collation,
                offset == null ? null : this.convertExpression(offset),
                fetch == null ? null : this.convertExpression(fetch)), false);
        if (!orderExprList.isEmpty()) {
            List<RexNode> exprs = new ArrayList<>();
            RelDataType rowType = bb.root.getRowType();
            int fieldCount = rowType.getFieldCount() - orderExprList.size();

            for (int i = 0; i < fieldCount; ++i) {
                exprs.add(this.rexBuilder.makeInputRef(bb.root, i));
            }

            bb.setRoot(LogicalProject.create(bb.root,
                    ImmutableList.of(),
                    exprs,
                    rowType.getFieldNames().subList(0, fieldCount)), false);
        }
    }
}
