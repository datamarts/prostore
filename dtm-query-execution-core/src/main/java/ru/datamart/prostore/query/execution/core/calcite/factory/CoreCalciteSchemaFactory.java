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
package ru.datamart.prostore.query.execution.core.calcite.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.factory.SchemaFactory;
import ru.datamart.prostore.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.datamart.prostore.query.calcite.core.schema.DtmTable;
import ru.datamart.prostore.query.calcite.core.schema.QueryableSchema;
import ru.datamart.prostore.query.execution.core.calcite.model.schema.CoreDtmTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("coreCalciteSchemaFactory")
public class CoreCalciteSchemaFactory extends CalciteSchemaFactory {

    @Autowired
    public CoreCalciteSchemaFactory(@Qualifier("coreSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new CoreDtmTable(schema, entity);
    }
}
