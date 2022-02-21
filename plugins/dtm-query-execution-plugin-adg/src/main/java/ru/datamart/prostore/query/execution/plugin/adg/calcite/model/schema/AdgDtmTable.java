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
package ru.datamart.prostore.query.execution.plugin.adg.calcite.model.schema;

import lombok.val;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.schema.DtmTable;
import ru.datamart.prostore.query.calcite.core.schema.QueryableSchema;

import java.util.ArrayList;
import java.util.List;

public class AdgDtmTable extends DtmTable {
    public AdgDtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(dtmSchema, entity, true);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), RelOptTableImpl.create(
                relOptTable.getRelOptSchema(),
                relOptTable.getRowType(),
                getTableNameWithoutSchema(relOptTable),
                relOptTable.getExpression(AdgDtmTable.class)
        ), new ArrayList<>());
    }

    private List<String> getTableNameWithoutSchema(RelOptTable relOptTable) {
        val qualifiedName = relOptTable.getQualifiedName();
        val size = qualifiedName.size();
        return size == 1 ? qualifiedName : qualifiedName.subList(size - 1, size);
    }
}
