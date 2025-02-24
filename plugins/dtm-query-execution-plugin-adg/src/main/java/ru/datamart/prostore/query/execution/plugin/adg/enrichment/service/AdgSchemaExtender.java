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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityUtils;
import ru.datamart.prostore.query.calcite.core.service.AbstractSchemaExtender;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adgSchemaExtender")
public class AdgSchemaExtender extends AbstractSchemaExtender {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgSchemaExtender(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    protected void extendLogicalEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic, String systemName) {
        val extendedDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        extendedDatamart.getEntities().add(entity);

        val extendedEntity = entity.copy();
        val helperTableNames = helperTableNamesFactory.create(systemName, datamartMnemonic, extendedEntity.getName());
        val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
        extendedEntityFields.addAll(getExtendedColumns());
        extendedEntity.setFields(extendedEntityFields);
        extendedDatamart.getEntities().add(EntityUtils.renameEntity(extendedEntity, helperTableNames.getHistory()));
        extendedDatamart.getEntities().add(EntityUtils.renameEntity(extendedEntity, helperTableNames.getStaging()));
        extendedDatamart.getEntities().add(EntityUtils.renameEntity(extendedEntity, helperTableNames.getActual()));
    }

    @Override
    protected void extendStandaloneEntity(Map<String, Datamart> extendedDatamarts, Entity entity, String datamartMnemonic) {
        val standaloneTableName = entity.getExternalTableLocationPath();
        val logicalDatamart = getOrCreateDatamart(extendedDatamarts, datamartMnemonic);
        logicalDatamart.getEntities().add(entity);
        logicalDatamart.getEntities().add(EntityUtils.renameEntity(entity, standaloneTableName));
    }

    private List<EntityField> getExtendedColumns() {
        return Arrays.asList(
                generateNewField(SYS_OP_FIELD),
                generateNewField(SYS_TO_FIELD),
                generateNewField(SYS_FROM_FIELD)
        );
    }

    private EntityField generateNewField(String name) {
        return EntityField.builder()
                .type(ColumnType.INT)
                .name(name)
                .build();
    }

}
