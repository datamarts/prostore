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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load;

import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import lombok.NonNull;
import org.apache.avro.Schema;

import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.EXT_SHARD_POSTFIX;
import static java.lang.String.format;

public class RestExtTableCreator implements ExtTableCreator {
    private static final String EXT_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (\n" +
                    "  %s\n" +
                    ")\n" +
                    "ENGINE = MergeTree()\n" +
                    "ORDER BY (%s)\n";

    private final DdlProperties ddlProperties;

    public RestExtTableCreator(DdlProperties ddlProperties) {
        this.ddlProperties = ddlProperties;
    }

    @Override
    public String generate(@NonNull String topic, @NonNull String table, @NonNull Schema schema, @NonNull String sortingKey) {
        String removeSysFrom = sortingKey.replaceAll(",\\s*sys_from", "");
        String columns = schema.getFields().stream()
                .map(f -> {
                    boolean isNullable = !sortingKey.contains(f.name());
                    return AdqmDdlUtil.avroFieldToString(f, isNullable);
                })
                .collect(Collectors.joining(", "));

        return format(EXT_SHARD_TEMPLATE, table + EXT_SHARD_POSTFIX, ddlProperties.getCluster(), columns, removeSysFrom);
    }
}
