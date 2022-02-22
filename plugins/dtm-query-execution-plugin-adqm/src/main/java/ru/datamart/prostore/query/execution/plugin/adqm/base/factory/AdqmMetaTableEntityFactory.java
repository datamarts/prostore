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
package ru.datamart.prostore.query.execution.plugin.adqm.base.factory;

import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableColumn;
import ru.datamart.prostore.query.execution.plugin.adqm.base.dto.metadata.AdqmTableEntity;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service("adqmMetadataEntityFactory")
public class AdqmMetaTableEntityFactory implements MetaTableEntityFactory<AdqmTableEntity> {

    public static final String IS_IN_SORTING_KEY = "is_in_sorting_key";
    public static final String IN_SORTING_KEY = "1";
    public static final String QUERY_PATTERN = "SELECT \n" +
            "  name as " + COLUMN_NAME + ", \n" +
            "  type as " + DATA_TYPE + ", \n" +
            "  is_in_sorting_key as " + IS_IN_SORTING_KEY + " \n" +
            "FROM cluster('%s', system.columns) \n" +
            "WHERE table = '%s' AND database = '%s__%s' \n" +
            "GROUP BY name, type, is_in_sorting_key \n" +
            "HAVING count() = (SELECT count(distinct shard_num) as shards_count \n" +
            "FROM system.clusters \n" +
            "WHERE cluster = '%s')";
    private static final String REGEX_TYPE_PATTERN = "Nullable\\((.*?)\\)";
    private final DatabaseExecutor adqmQueryExecutor;
    private final DdlProperties ddlProperties;


    @Autowired
    public AdqmMetaTableEntityFactory(DatabaseExecutor adqmQueryExecutor,
                                      DdlProperties ddlProperties) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.ddlProperties = ddlProperties;
    }

    @Override
    public Future<Optional<AdqmTableEntity>> create(String envName, String schema, String table) {
        val cluster  = ddlProperties.getCluster();
        String query = String.format(QUERY_PATTERN, cluster, table, envName, schema, cluster);
        return adqmQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture(result.isEmpty()
                        ? Optional.empty()
                        : Optional.of(transformToAdqmEntity(result))));
    }

    private AdqmTableEntity transformToAdqmEntity(List<Map<String, Object>> mapList) {
        AdqmTableEntity result = new AdqmTableEntity();
        List<String> sortedKeys = new ArrayList<>();
        List<AdqmTableColumn> columns = mapList.stream()
                .peek(map -> {
                    if (IN_SORTING_KEY.equals(map.get(IS_IN_SORTING_KEY).toString())) {
                        sortedKeys.add(map.get(COLUMN_NAME).toString());
                    }
                })
                .map(this::transformColumn).collect(Collectors.toList());
        result.setSortedKeys(sortedKeys);
        result.setColumns(columns);
        return result;
    }

    private AdqmTableColumn transformColumn(Map<String, Object> map) {
        String type;
        boolean nullable;
        String mapType = map.get(DATA_TYPE).toString();
        Pattern pattern = Pattern.compile(REGEX_TYPE_PATTERN);
        Matcher matcher = pattern.matcher(mapType);
        if (matcher.matches()) {
            type = matcher.group(1);
            nullable = true;
        } else {
            type = mapType;
            nullable = false;
        }
        return AdqmTableColumn.builder()
                .name(map.get(COLUMN_NAME).toString())
                .type(type)
                .nullable(nullable)
                .build();
    }
}
