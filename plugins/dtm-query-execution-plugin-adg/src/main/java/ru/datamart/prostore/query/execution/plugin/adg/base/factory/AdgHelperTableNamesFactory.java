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
package ru.datamart.prostore.query.execution.plugin.adg.base.factory;

import ru.datamart.prostore.query.execution.plugin.adg.base.dto.AdgHelperTableNames;
import org.springframework.stereotype.Component;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.*;

@Component
public class AdgHelperTableNamesFactory {

    public AdgHelperTableNames create(String envName, String datamartMnemonic, String tableName) {
        String prefix = getTablePrefix(envName, datamartMnemonic);
        return new AdgHelperTableNames(
                prefix + tableName + STAGING_POSTFIX,
                prefix + tableName + HISTORY_POSTFIX,
                prefix + tableName + ACTUAL_POSTFIX,
                prefix
        );
    }

    public String getTablePrefix(String envName, String datamartMnemonic) {
        return envName + "__" + datamartMnemonic + "__";
    }
}
