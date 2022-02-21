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
package ru.datamart.prostore.query.execution.plugin.api.dml;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;
import io.vertx.core.json.JsonArray;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class LlrEstimateUtilsTest {

    @Test
    void shouldReturnNullWhenEmptyResultSet() {
        // act
        String result = LlrEstimateUtils.extractPlanJson(Collections.emptyList());

        // assert
        assertNull(result);
    }

    @Test
    void shouldReturnNullWhenEmptyNullSet() {
        // act
        String result = LlrEstimateUtils.extractPlanJson(null);

        // assert
        assertNull(result);
    }

    @Test
    void shouldReturnNullWhenMoreThanOneResults() {
        // act
        String result = LlrEstimateUtils.extractPlanJson(Arrays.asList(new HashMap<>(), new HashMap<>()));

        // assert
        assertNull(result);
    }

    @Test
    void shouldReturnNullWhenResultHasNoPlan() {
        // arrange
        HashMap<String, Object> item = new HashMap<>();

        // act
        String result = LlrEstimateUtils.extractPlanJson(Arrays.asList(item));

        // assert
        assertNull(result);
    }

    @Test
    void shouldTransformToStringWhenResultHasPlanJson() {
        // arrange
        String json = "[{\"test\":true}]";
        HashMap<String, Object> item = new HashMap<>();
        JsonArray jsonArray = new JsonArray(json);
        item.put(LlrEstimateUtils.LLR_ESTIMATE_METADATA.getName(), jsonArray);

        // act
        String result = LlrEstimateUtils.extractPlanJson(Arrays.asList(item));

        // assert
        assertEquals(json, result);
    }

    @Test
    void shouldPrepareResultJson() {
        // arrange
        String json = "[{\"test\":true}]";
        String query = "select * from users";

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":\"select * from users\"}", result);
    }

    @Test
    void shouldPutNullAsPlanWhenNoPlan() {
        // arrange
        String query = "select * from users";

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, null);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":null,\"query\":\"select * from users\"}", result);
    }

    @Test
    void shouldPutNullAsQueryJson() {
        // arrange
        String json = "[{\"test\":true}]";
        String query = null;

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":null}", result);
    }

    @Test
    void shouldRemoveNewLinesAndPrepareResultJson() {
        // arrange
        String json = "[{\"test\":true}]";
        String query = "select\n*\rfrom\r\nusers";

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":\"select * from users\"}", result);
    }

    @Test
    void shouldNotRemoveWhitespacesAndPrepareResultJson() {
        // arrange
        String json = "[{\"test\":true}]";
        String query = "select       \n     *, '           '     \r       from            \r\nusers";

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":\"select             *, '           '             from             users\"}", result);
    }

    @Test
    void shouldAddEscapeToQuotesOnQuery() {
        // arrange
        String json = "[{\"test\":true}]";
        String query = "select * from \"users\" where 'something' = \"test.users.id\"";

        // act
        String result = LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json);

        // assert
        assertEquals("{\"plugin\":\"ADB\",\"estimation\":[{\"test\":true}],\"query\":\"select * from \\\"users\\\" where 'something' = \\\"test.users.id\\\"\"}", result);
    }

    @Test
    void shouldFailWhenInvalidPlanJson() {
        // arrange
        String json = "[";
        String query = "select * from users";

        // act assert
        assertThrows(DtmException.class, () -> LlrEstimateUtils.prepareResultJson(SourceType.ADB, query, json));
    }
}