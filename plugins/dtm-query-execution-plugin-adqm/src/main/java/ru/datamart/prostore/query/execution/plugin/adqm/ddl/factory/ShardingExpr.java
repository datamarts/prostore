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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory;

import java.util.List;

public enum ShardingExpr {
    INT_ADD,
    CITY_HASH_64;

    public String getValue(List<String> shardingKeys) {
        switch (this) {
            case INT_ADD:
                return String.join("+", shardingKeys);
            case CITY_HASH_64:
            default:
                return String.format("cityHash64(%s)", String.join(", ", shardingKeys));
        }
    }
}
