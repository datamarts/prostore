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
package ru.datamart.prostore.query.calcite.core.schema;

import lombok.var;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;

import java.util.ArrayList;

public abstract class DtmTable extends AbstractQueryableTable implements TranslatableTable {
    protected final QueryableSchema dtmSchema;
    protected final Entity entity;
    private final boolean allowVarcharSize;

    protected DtmTable(QueryableSchema dtmSchema, Entity entity, boolean allowVarcharSize) {
        super(Object[].class);
        this.dtmSchema = dtmSchema;
        this.entity = entity;
        this.allowVarcharSize = allowVarcharSize;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        //TODO: complete the task of executing the request
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        entity.getFields().forEach(it -> {
            boolean nullable = it.getNullable() != null && it.getNullable();
            var columnSize = it.getSize();
            var columnAccuracy = it.getAccuracy();
            switch (it.getType()) {
                case VARCHAR:
                case CHAR:
                case LINK:
                    if (!allowVarcharSize && columnSize != null) {
                        columnSize = null;
                    }
                    break;
                case UUID:
                    if (allowVarcharSize && columnSize == null) {
                        columnSize = CoreConstants.UUID_SIZE;
                    }
                    break;
                case TIME:
                case TIMESTAMP:
                    if (columnAccuracy != null) {
                        columnSize = columnAccuracy;
                        columnAccuracy = null;
                    }
                    break;
                default:
                    break;
            }
            addToBuilder(builder, it.getName(), it.getType(), nullable, columnSize, columnAccuracy);
        });
        return builder.build();
    }

    private void addToBuilder(RelDataTypeFactory.Builder builder, String name, ColumnType type, boolean nullable, Integer size, Integer accuracy) {
        if (size != null && accuracy != null) {
            builder.add(name, CalciteUtil.valueOf(type), size, accuracy)
                    .nullable(nullable);
        } else if (size != null) {
            builder.add(name, CalciteUtil.valueOf(type), size)
                    .nullable(nullable);
        } else {
            builder.add(name, CalciteUtil.valueOf(type))
                    .nullable(nullable);
        }
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable, new ArrayList<>());
    }

    public Entity getEntity() {
        return entity;
    }
}
