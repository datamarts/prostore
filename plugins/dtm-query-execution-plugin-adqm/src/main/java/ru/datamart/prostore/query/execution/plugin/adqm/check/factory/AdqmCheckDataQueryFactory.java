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
package ru.datamart.prostore.query.execution.plugin.adqm.check.factory;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;

import java.util.stream.Collectors;

@Component
public class AdqmCheckDataQueryFactory {
    private static final String COUNT_QUERY_PATTERN = "SELECT count(1) as %s\n" +
            "  FROM %s__%s.%s_actual FINAL\n" +
            "  WHERE (sys_from >= %d AND sys_from <= %d)\n" +
            "    OR\n" +
            "  (sys_to >= %d AND sys_to <= %d AND sys_op = 1)";

    private static final String HASH_QUERY_PATTERN = "SELECT \n" +
            "  sum(\n" +
            "   intDiv(\n" +
            "    reinterpretAsUInt32(\n" +
            "      lower(\n" +
            "        hex(\n" +
            "          MD5(\n" +
            "            %s\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "    ), %d\n" +
            "   )\n" +
            "  ) as %s\n" +
            "FROM %s__%s.%s_actual FINAL\n" +
            "  WHERE (sys_from >= %d AND sys_from <= %d)\n" +
            "    OR\n" +
            "  (sys_to >= %d AND sys_to <= %d AND sys_op = 1)";

    private static final String HASH_SNAPSHOT_QUERY_PATTERN = "SELECT \n" +
            "  sum(\n" +
            "   intDiv(\n" +
            "    reinterpretAsUInt32(\n" +
            "      lower(\n" +
            "        hex(\n" +
            "          MD5(\n" +
            "            %s\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "    ), %d\n" +
            "   )\n" +
            "  ) as %s\n" +
            "FROM %s__%s.%s_actual FINAL\n" +
            "  WHERE sys_from <= %d\n" +
            "        and \n" +
            "        sys_to >= %d";

    public String createCheckDataByCountQuery(CheckDataByCountRequest request, String resultColumnName) {
        return String.format(COUNT_QUERY_PATTERN,
                resultColumnName,
                request.getEnvName(),
                request.getEntity().getSchema(),
                request.getEntity().getName(),
                request.getCnFrom(),
                request.getCnTo(),
                request.getCnFrom() - 1,
                request.getCnTo() - 1);
    }

    public String createCheckDataByHashInt32Query(CheckDataByHashInt32Request request, String resultColumnName) {
        val entity = request.getEntity();
        val colQuery = getColumnsQuery(request);
        return String.format(HASH_QUERY_PATTERN,
                colQuery,
                request.getNormalization(),
                resultColumnName,
                request.getEnvName(),
                entity.getSchema(),
                entity.getName(),
                request.getCnFrom(),
                request.getCnTo(),
                request.getCnFrom() - 1,
                request.getCnTo() - 1);
    }

    public String createCheckDataSnapshotByHashInt32Query(CheckDataByHashInt32Request request, String resultColumnName) {
        val entity = request.getEntity();
        val colQuery = getColumnsQuery(request);
        return String.format(HASH_SNAPSHOT_QUERY_PATTERN,
                colQuery,
                request.getNormalization(),
                resultColumnName,
                request.getEnvName(),
                entity.getSchema(),
                entity.getName(),
                request.getCnTo(),
                request.getCnTo());
    }

    private String getColumnsQuery(CheckDataByHashInt32Request request) {
        val columns = request.getColumns().stream()
                .map(column -> String.format("ifNull(toString(%s),'')", column))
                .collect(Collectors.toList());
        return columns.size() > 1
                ? String.format("concat(%s)", String.join(",';',", columns))
                : columns.get(0);
    }
}
