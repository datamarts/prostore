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
package ru.datamart.prostore.query.calcite.core.service;

import lombok.val;
import org.apache.commons.lang3.BooleanUtils;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractSchemaExtender implements SchemaExtender {

    @Override
    public List<Datamart> extendSchema(List<Datamart> logicalSchemas, String env) {
        Map<String, Datamart> extendedDatamarts = new HashMap<>();
        String defaultDatamart = null;
        for (val datamart : logicalSchemas) {
            val datamartMnemonic = datamart.getMnemonic();
            if (BooleanUtils.isTrue(datamart.getIsDefault())) {
                defaultDatamart = datamartMnemonic;
            }

            for (Entity entity : datamart.getEntities()) {
                switch (entity.getEntityType()) {
                    case WRITEABLE_EXTERNAL_TABLE:
                    case READABLE_EXTERNAL_TABLE:
                        extendStandaloneEntity(extendedDatamarts, entity, datamartMnemonic);
                        break;
                    default:
                        extendLogicalEntity(extendedDatamarts, entity, datamartMnemonic, env);
                        break;
                }
            }
        }

        if (defaultDatamart != null && extendedDatamarts.containsKey(defaultDatamart)) {
            extendedDatamarts.get(defaultDatamart).setIsDefault(true);
        }
        return new ArrayList<>(extendedDatamarts.values());
    }

    protected abstract void extendLogicalEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic, String systemName);

    protected abstract void extendStandaloneEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic);

    protected Datamart getOrCreateDatamart(Map<String, Datamart> extendedDatamarts, String datamartName) {
        return extendedDatamarts.computeIfAbsent(datamartName,
                key -> Datamart.builder().mnemonic(key).isDefault(false).entities(new ArrayList<>()).build());
    }
}
