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
package ru.datamart.prostore.query.execution.plugin.adb.calcite.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.factory.SchemaFactory;
import ru.datamart.prostore.query.calcite.core.factory.impl.CalciteSchemaFactory;
import ru.datamart.prostore.query.calcite.core.schema.DtmTable;
import ru.datamart.prostore.query.calcite.core.schema.QueryableSchema;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.model.schema.AdbDtmTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbCalciteSchemaFactory")
public class AdbCalciteSchemaFactory extends CalciteSchemaFactory {

    @Autowired
    public AdbCalciteSchemaFactory(@Qualifier("adbSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new AdbDtmTable(schema, entity);
    }
}
