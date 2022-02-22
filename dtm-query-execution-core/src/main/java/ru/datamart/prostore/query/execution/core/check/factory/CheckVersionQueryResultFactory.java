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
package ru.datamart.prostore.query.execution.core.check.factory;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class CheckVersionQueryResultFactory {

    public static final String COMPONENT_NAME_COLUMN = "component_name";
    public static final String VERSION_COLUMN = "version";

    public QueryResult create(List<VersionInfo> versionInfos) {
        return QueryResult.builder()
                .metadata(getMetadata())
                .result(createResultList(versionInfos))
                .build();
    }

    private List<ColumnMetadata> getMetadata() {
        return Arrays.asList(
                ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()
        );
    }

    private List<Map<String, Object>> createResultList(List<VersionInfo> versionInfos) {
        List<Map<String, Object>> result = new ArrayList<>();
        versionInfos.forEach(v -> {
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put(COMPONENT_NAME_COLUMN, v.getName());
            resultMap.put(VERSION_COLUMN, v.getVersion());
            result.add(resultMap);
        });
        return result;
    }
}
