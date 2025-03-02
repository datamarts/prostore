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
package ru.datamart.prostore.query.execution.plugin.adb.check;

import org.junit.jupiter.api.Test;
import ru.datamart.prostore.query.execution.plugin.adb.check.service.AdbCheckDataServiceWithHistory;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AdbCheckDataServiceWithHistoryTest {
    private final AdbCheckDataServiceWithHistory checkDataService = new AdbCheckDataServiceWithHistory();

    @Test
    void checkDataByHashTest() {
        checkDataService.checkDataByHashInt32(CheckDataByHashInt32Request.builder().build())
                .onComplete(ar ->
                        assertTrue(ar.failed()));
    }

    @Test
    void checkDataSnapshotByHashTest() {
        checkDataService.checkDataByHashInt32(CheckDataByHashInt32Request.builder().build())
                .onComplete(ar ->
                        assertTrue(ar.failed()));
    }

    @Test
    void checkDataByCountTest() {
        checkDataService.checkDataByCount(CheckDataByCountRequest.builder().build())
                .onComplete(ar ->
                        assertTrue(ar.failed()));
    }
}
