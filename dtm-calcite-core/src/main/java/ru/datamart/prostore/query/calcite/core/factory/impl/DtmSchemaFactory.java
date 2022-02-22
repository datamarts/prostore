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
package ru.datamart.prostore.query.calcite.core.factory.impl;

import ru.datamart.prostore.query.calcite.core.factory.SchemaFactory;
import ru.datamart.prostore.query.calcite.core.schema.QueryableSchema;
import ru.datamart.prostore.query.calcite.core.schema.dialect.DtmConvention;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;

public abstract class DtmSchemaFactory implements SchemaFactory {
    @Override
    public QueryableSchema create(SchemaPlus parentSchema, Datamart datamart) {
        Expression expression = Schemas.subSchemaExpression(parentSchema, datamart.getMnemonic(), QueryableSchema.class);
        DtmConvention convention = createDtmConvention(datamart, expression);
        return new QueryableSchema(convention);
    }

    protected abstract DtmConvention createDtmConvention(Datamart datamart, Expression expression);
}
