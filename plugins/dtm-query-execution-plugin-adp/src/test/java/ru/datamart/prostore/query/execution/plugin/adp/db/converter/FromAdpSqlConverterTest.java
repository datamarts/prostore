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
package ru.datamart.prostore.query.execution.plugin.adp.db.converter;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.model.ddl.ColumnType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FromAdpSqlConverterTest {
    private SqlTypeConverter typeConverter;
    private String charVal;
    private Long intVal;
    private Long bigintVal;
    private Double doubleVal;
    private Float floatVal;
    private Long dateLongVal;
    private Long timeLongVal;
    private Long timestampLongVal;
    private Boolean booleanVal;
    private String uuidStrVal;
    private Map<String, Object> objMapVal;
    Map<ColumnType, Object> expectedValues = new HashMap<>();

    @BeforeEach
    void setUp() {
        typeConverter = new FromAdpSqlConverter();
        charVal = "111";
        intVal = 1L;
        bigintVal = 1L;
        doubleVal = 1.0d;
        floatVal = 1.0f;
        dateLongVal = 18540L;
        timeLongVal = 58742894000000L;
        timestampLongVal = 1601878742000L;
        booleanVal = true;
        uuidStrVal = "a7180dcb-b286-4168-a34a-eb378a69abd4";
        objMapVal = new HashMap<>();
        objMapVal.put("id", 1);

        expectedValues.put(ColumnType.VARCHAR, charVal);
        expectedValues.put(ColumnType.CHAR, charVal);
        expectedValues.put(ColumnType.LINK, charVal);
        expectedValues.put(ColumnType.INT, intVal);
        expectedValues.put(ColumnType.INT32, intVal);
        expectedValues.put(ColumnType.BIGINT, bigintVal);
        expectedValues.put(ColumnType.DOUBLE, doubleVal);
        expectedValues.put(ColumnType.FLOAT, floatVal);
        expectedValues.put(ColumnType.DATE, dateLongVal.intValue());
        expectedValues.put(ColumnType.TIME, timeLongVal / 1000);
        expectedValues.put(ColumnType.TIMESTAMP, timestampLongVal * 1000);
        expectedValues.put(ColumnType.BOOLEAN, booleanVal);
        expectedValues.put(ColumnType.UUID, UUID.fromString(uuidStrVal));
        expectedValues.put(ColumnType.ANY, JsonObject.mapFrom(objMapVal));
    }

    @Test
    void convert() {
        assertWithClass(ColumnType.VARCHAR, charVal, String.class);
        assertWithClass(ColumnType.CHAR, charVal, String.class);
        assertWithClass(ColumnType.LINK, charVal, String.class);
        assertWithClass(ColumnType.INT, intVal, Long.class);
        assertWithClass(ColumnType.INT32, intVal, Long.class);
        assertWithClass(ColumnType.BIGINT, intVal, Long.class);
        assertWithClass(ColumnType.DOUBLE, doubleVal, Double.class);
        assertWithClass(ColumnType.FLOAT, floatVal, Float.class);
        assertAll("Date converting",
                () -> assertEquals(expectedValues.get(ColumnType.DATE), typeConverter.convert(ColumnType.DATE, LocalDate.ofEpochDay(dateLongVal))),
                () -> assertTrue(typeConverter.convert(ColumnType.DATE, LocalDate.ofEpochDay(dateLongVal)) instanceof Integer)
        );
        assertAll("Time converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIME), typeConverter.convert(ColumnType.TIME, LocalTime.ofNanoOfDay(timeLongVal))),
                () -> assertTrue(typeConverter.convert(ColumnType.TIME, LocalTime.ofNanoOfDay(timeLongVal)) instanceof Number)
        );
        assertAll("Timestamp converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIMESTAMP), typeConverter.convert(ColumnType.TIMESTAMP,
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampLongVal), CoreConstants.CORE_ZONE_ID))),
                () -> assertTrue(typeConverter.convert(ColumnType.TIMESTAMP,
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampLongVal), CoreConstants.CORE_ZONE_ID)) instanceof Long)
        );
        assertWithClass(ColumnType.BOOLEAN, booleanVal, Boolean.class);
        assertWithClass(ColumnType.UUID, uuidStrVal, UUID.class);
        assertAll("Any converting",
                () -> assertEquals(expectedValues.get(ColumnType.ANY), typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal))),
                () -> assertTrue(typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal)) instanceof JsonObject)
        );
    }

    @Test
    void convertWithNull() {
        for (ColumnType type: expectedValues.keySet()) {
            assertNull(typeConverter.convert(type, null));
        }
    }

    private void assertWithClass(ColumnType columnType, Object value, Class<?> convertedClass) {
        assertAll(columnType.name() + " converting",
                () -> assertEquals(expectedValues.get(columnType), typeConverter.convert(columnType, value)),
                () -> assertTrue(convertedClass.isInstance(typeConverter.convert(columnType, value)))
        );
    }
}
