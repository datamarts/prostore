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
package ru.datamart.prostore.query.execution.core.eddl.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Запрос удаления внешней загрузки
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DropUploadExternalTableQuery extends EddlQuery {

    public DropUploadExternalTableQuery() {
        super(EddlAction.DROP_UPLOAD_EXTERNAL_TABLE);
    }

    public DropUploadExternalTableQuery(String schemaName, String tableName) {
        super(EddlAction.DROP_UPLOAD_EXTERNAL_TABLE, schemaName, tableName);
    }
}
