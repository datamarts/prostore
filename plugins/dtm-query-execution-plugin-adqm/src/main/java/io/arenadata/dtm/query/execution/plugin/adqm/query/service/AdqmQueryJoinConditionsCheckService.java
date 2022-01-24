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
package io.arenadata.dtm.query.execution.plugin.adqm.query.service;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.query.dto.AdqmJoinQuery;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.extractor.SqlJoinConditionExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AdqmQueryJoinConditionsCheckService {

    private final SqlJoinConditionExtractor joinConditionExtractor;

    @Autowired
    public AdqmQueryJoinConditionsCheckService(SqlJoinConditionExtractor joinConditionExtractor) {
        this.joinConditionExtractor = joinConditionExtractor;
    }

    public boolean isJoinConditionsCorrect(AdqmCheckJoinRequest request) {
        try {
            List<AdqmJoinQuery> queryJoins = joinConditionExtractor.extract(request.getRelNode());
            if (queryJoins.isEmpty()) {
                return true;
            }

            Map<String, List<String>> tableToShardingKeysMap = new HashMap<>();
            for (Datamart datamart : request.getSchema()) {
                String mnemonic = datamart.getMnemonic();
                for (Entity entity : datamart.getEntities()) {
                    List<String> shardingKeys = entity.getFields().stream()
                            .filter(f -> f.getShardingOrder() != null)
                            .sorted(Comparator.comparingInt(EntityField::getShardingOrder))
                            .map(EntityField::getName)
                            .collect(Collectors.toList());
                    tableToShardingKeysMap.put(getTableName(mnemonic, entity.getName()), shardingKeys);
                }
            }

            for (AdqmJoinQuery join : queryJoins) {
                if (!isValidJoin(join, tableToShardingKeysMap)) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            log.error("ADQM join is not valid. Failed on validation.", e);
            throw new DtmException(e);
        }
    }

    private boolean isValidJoin(AdqmJoinQuery join, Map<String, List<String>> tableToShardingKeysMap) {
        if (join.isHasNonEquiConditions()) {
            log.error("ADQM join is not valid. Query has non equi condition in join.");
            return false;
        }

        if (join.getLeftConditionColumns().size() != join.getRightConditionColumns().size() || join.getLeftConditionColumns().size() != 1) {
            log.error("ADQM join is not valid. Join has wrong condition count. (left keys: {}, right keys: {})",
                    join.getLeftConditionColumns().size(), join.getRightConditionColumns().size());
            return false;
        }

        List<String> leftShardingKeys = tableToShardingKeysMap.get(getTableName(join.getLeft()));
        List<String> rightShardingKeys = tableToShardingKeysMap.get(getTableName(join.getRight()));
        if (leftShardingKeys == null || rightShardingKeys == null) {
            log.error("ADQM join is not valid. Sharding keys must be found got [{} by {}] and [{} by {}]",
                    leftShardingKeys, getTableName(join.getLeft()), rightShardingKeys, getTableName(join.getRight()));
            throw new DtmException("ADQM join is not valid. Not found sharding keys for join tables.");
        }

        if (leftShardingKeys.size() != join.getLeftConditionColumns().size()) {
            log.error("ADQM join is not valid. LEFT keys in join not equal to table sharding keys size. (sharding keys: {}, join keys: {})",
                    leftShardingKeys.size(), join.getLeftConditionColumns().size());
            return false;
        }

        if (rightShardingKeys.size() != join.getRightConditionColumns().size()) {
            log.error("ADQM join is not valid. RIGHT keys in join not equal to table sharding keys size. (sharding keys: {}, join keys: {})",
                    rightShardingKeys.size(), join.getRightConditionColumns().size());
            return false;
        }

        for (int i = 0; i < leftShardingKeys.size(); i++) {
            List<String> leftColumns = join.getLeftConditionColumns().get(i);
            if (leftColumns.size() > 1) {
                log.error("ADQM join is not valid. LEFT condition in join has multiple columns [{}] in condition [{}]", leftColumns.size(), i);
                return false;
            }

            String leftShardingColumn = leftShardingKeys.get(i);
            if (!leftShardingColumn.equals(leftColumns.get(0))) {
                log.error("ADQM join is not valid. LEFT [{}] join key {} not equal to sharding key [{}]", i, leftColumns, leftShardingColumn);
                return false;
            }

            List<String> rightColumns = join.getRightConditionColumns().get(i);
            if (rightColumns.size() > 1) {
                log.error("ADQM join is not valid. RIGHT condition in join has multiple columns [{}] in condition [{}]", rightColumns.size(), i);
                return false;
            }

            String rightShardingColumn = rightShardingKeys.get(i);
            if (!rightShardingColumn.equals(rightColumns.get(0))) {
                log.error("ADQM join is not valid. RIGHT [{}] join key {} not equal to sharding key [{}]", i, rightColumns, rightShardingColumn);
                return false;
            }
        }

        return true;
    }

    private static String getTableName(String schema, String name) {
        return schema + "." + name;
    }

    private static String getTableName(RelNode relNode) {
        if (relNode instanceof TableScan) {
            return String.join(".", relNode.getTable().getQualifiedName());
        }

        if (relNode instanceof Project) {
            List<RelNode> inputs = relNode.getInputs();
            if (inputs.size() != 1) {
                throw new DtmException("ADQM join has wrong input nodes.");
            }

            RelNode inputRel = inputs.get(0);
            if (!(inputRel instanceof TableScan)) {
                throw new DtmException("ADQM join has wrong input nodes.");
            }

            return String.join(".", inputRel.getTable().getQualifiedName());
        }

        throw new DtmException("ADQM join has wrong input nodes.");
    }
}
