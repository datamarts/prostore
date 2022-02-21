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
package ru.datamart.prostore.query.calcite.core.factory.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.factory.SchemaFactory;
import ru.datamart.prostore.query.calcite.core.schema.DtmTable;
import ru.datamart.prostore.query.calcite.core.schema.QueryableSchema;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import org.apache.calcite.schema.SchemaPlus;

public abstract class CalciteSchemaFactory {
    private final SchemaFactory schemaFactory;

    public CalciteSchemaFactory(SchemaFactory schemaFactory) {
        this.schemaFactory = schemaFactory;
    }

    public SchemaPlus addSchema(SchemaPlus parent, Datamart root) {
        QueryableSchema dtmSchema = schemaFactory.create(parent, root);
        SchemaPlus schemaPlus = parent.add(root.getMnemonic(), dtmSchema);
        root.getEntities().forEach(it -> {
            try {
                DtmTable table = createTable(dtmSchema, it);
                schemaPlus.add(it.getName(), table);
            } catch (Exception e) {
                throw new DtmException("Table initialization error $metaTable", e);
            }
        });
        return schemaPlus;
    }

    protected abstract DtmTable createTable(QueryableSchema schema, Entity entity);
}
