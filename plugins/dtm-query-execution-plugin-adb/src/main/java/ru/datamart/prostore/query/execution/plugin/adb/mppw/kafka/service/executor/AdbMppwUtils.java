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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor;

import lombok.Data;
import lombok.val;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;

import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.STAGING_TABLE_SUFFIX;

public final class AdbMppwUtils {
    private static final String AUTO_CREATE_SYS_OP_ENABLE_OPTION = "auto.create.sys_op.enable";

    private AdbMppwUtils() {
    }

    public static TableSchema getTableSchema(Entity entity) {
        if (entity.getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE) {
            val schemaAndTableName = entity.getExternalTableLocationPath().split("\\.");
            return new TableSchema(schemaAndTableName[0], schemaAndTableName[1]);
        }

        return new TableSchema(entity.getSchema(), entity.getName() + STAGING_TABLE_SUFFIX);
    }

    public static boolean isWithSysOpField(Map<String, String> optionsMap) {
        val createSysOp = optionsMap.get(AUTO_CREATE_SYS_OP_ENABLE_OPTION);
        if (createSysOp == null) {
            return true;
        }

        return Boolean.parseBoolean(createSysOp);
    }

    @Data
    public static class TableSchema {
        private final String schema;
        private final String table;
    }
}
