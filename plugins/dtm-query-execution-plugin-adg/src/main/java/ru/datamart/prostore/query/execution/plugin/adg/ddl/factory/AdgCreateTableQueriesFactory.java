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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adg.base.dto.AdgTables;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.AdgSpace;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.Space;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.AdgUtils;
import ru.datamart.prostore.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.TableEntitiesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.UnaryOperator;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;

@Service("adgCreateTableQueriesFactory")
public class AdgCreateTableQueriesFactory implements CreateTableQueriesFactory<AdgTables<AdgSpace>> {
    private final TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory;

    @Autowired
    public AdgCreateTableQueriesFactory(TableEntitiesFactory<AdgTables<Space>> tableEntitiesFactory) {
        this.tableEntitiesFactory = tableEntitiesFactory;
    }

    @Override
    public AdgTables<AdgSpace> create(Entity entity, String envName) {
        AdgTables<Space> tableEntities = tableEntitiesFactory.create(entity, envName);
        UnaryOperator<String> getName = postfix ->
                AdgUtils.getSpaceName(envName, entity.getSchema(), entity.getName(),
                        postfix);
        return new AdgTables<>(
                new AdgSpace(getName.apply(ACTUAL_POSTFIX), tableEntities.getActual()),
                new AdgSpace(getName.apply(HISTORY_POSTFIX), tableEntities.getHistory()),
                new AdgSpace(getName.apply(STAGING_POSTFIX), tableEntities.getStaging())
        );
    }
}
