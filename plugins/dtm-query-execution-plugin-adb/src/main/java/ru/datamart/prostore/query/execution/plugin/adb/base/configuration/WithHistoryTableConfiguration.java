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
package ru.datamart.prostore.query.execution.plugin.adb.base.configuration;

import ru.datamart.prostore.query.execution.plugin.adb.check.service.AdbCheckDataServiceWithHistory;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.TruncateQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl.TruncateQueryWithHistoryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.impl.MppwWithHistoryTableRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.factory.RollbackWithHistoryTableRequestFactory;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "adb.with-history-table", havingValue = "true")
public class WithHistoryTableConfiguration {

    public WithHistoryTableConfiguration() {
        log.info("With history table");
    }

    @Bean
    public RollbackRequestFactory<AdbRollbackRequest> adbRollbackRequestFactory() {
        return new RollbackWithHistoryTableRequestFactory();
    }

    @Bean
    public MppwRequestFactory adbMppwRequestFactory() {
        return new MppwWithHistoryTableRequestFactory();
    }

    @Bean
    public QueryExtendService adbDmlExtendService() {
        return new AdbDmlQueryExtendWithHistoryService();
    }

    @Bean
    public CheckDataService adbCheckDataService() {
        return new AdbCheckDataServiceWithHistory();
    }

    @Bean
    public TruncateQueryFactory adbTruncateHistoryQueryFactory(@Qualifier("adbSqlDialect") SqlDialect sqlDialect){
        return new TruncateQueryWithHistoryFactory(sqlDialect);
    }

}
