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
package io.arenadata.dtm.query.execution.plugin.adqm.base.factory;

import io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata.AdqmHelperTableNames;
import org.springframework.stereotype.Component;

@Component
public class AdqmHelperTableNamesFactory {

    public AdqmHelperTableNames create(String envName, String datamartMnemonic, String tableName) {
        String schema = envName + "__" + datamartMnemonic;
        return new AdqmHelperTableNames(
            schema,
            tableName + "_actual",
            tableName + "_actual_shard"
        );
    }
}
