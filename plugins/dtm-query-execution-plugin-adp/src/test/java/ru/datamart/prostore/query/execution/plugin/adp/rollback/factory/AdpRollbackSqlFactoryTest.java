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
package ru.datamart.prostore.query.execution.plugin.adp.rollback.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AdpRollbackSqlFactoryTest {

    @Test
    void shouldBeCorrectRollbackSql() {
        // arrange
        String datamart = "datamart";
        Entity entity = Entity.builder()
                .name("adpTable")
                .build();
        Long sysCn = 10L;

        // act
        String rollbackSql = AdpRollbackSqlFactory.getRollbackSql(datamart, entity, sysCn);

        // assert
        Assertions.assertThat(rollbackSql).isEqualToNormalizingNewlines("TRUNCATE datamart.adpTable_staging;\n" +
                "DELETE FROM datamart.adpTable_actual WHERE sys_from = 10;\n" +
                "UPDATE datamart.adpTable_actual SET sys_to = NULL, sys_op = 0 WHERE sys_to = 9;");
    }

}