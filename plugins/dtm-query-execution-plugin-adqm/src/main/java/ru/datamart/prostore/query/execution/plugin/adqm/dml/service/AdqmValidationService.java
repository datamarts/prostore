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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.execution.plugin.adqm.query.dto.AdqmCheckJoinRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import org.springframework.stereotype.Service;

@Service("adqmValidationService")
public class AdqmValidationService implements LlrValidationService {

    private final AdqmQueryJoinConditionsCheckService joinConditionsCheckService;

    public AdqmValidationService(AdqmQueryJoinConditionsCheckService joinConditionsCheckService) {
        this.joinConditionsCheckService = joinConditionsCheckService;
    }

    @Override
    public void validate(QueryParserResponse queryParserResponse) {
        if (!joinConditionsCheckService.isJoinConditionsCorrect(new AdqmCheckJoinRequest(queryParserResponse.getRelNode().rel, queryParserResponse.getSchema()))) {
            throw new DataSourceException("Clickhouse’s global join is restricted");
        }
    }

}
