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
package ru.datamart.prostore.query.execution.plugin.adqm.converter;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.converter.SqlTypeConverter;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.FromAdqmSqlTypeConverter;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class FromAdqmSqlTypeConverterTest {
    private SqlTypeConverter typeConverter;
    private String charVal;
    private Long intVal;
    private Long bigintVal;
    private Double doubleVal;
    private Float floatVal;
    private Long dateLongVal;
    private String dateStrVal;
    private Long timeLongVal;
    private Long timestampLongVal;
    private String timestampStrVal;
    private Boolean booleanVal;
    private String uuidStrVal;
    private BigInteger bigInteger;
    private Map<String, Object> objMapVal;

    @BeforeEach
    void setUp() {
        typeConverter = new FromAdqmSqlTypeConverter();
        charVal = "111";
        intVal = 1L;
        bigintVal = 1L;
        doubleVal = 1.0d;
        floatVal = 1.0f;
        dateLongVal = 18540L;
        dateStrVal = "2020-10-05";
        timeLongVal = 58742894123456L;
        timestampLongVal = 1601878742000L;
        timestampStrVal = "2020-10-05T14:15:16Z";
        booleanVal = true;
        uuidStrVal = "a7180dcb-b286-4168-a34a-eb378a69abd4";
        bigInteger = BigInteger.ONE;
        objMapVal = new HashMap<>();
        objMapVal.put("id", 1);
    }

    @Test
    void convert() {
        Map<ColumnType, Object> expectedValues = new HashMap<>();
        expectedValues.put(ColumnType.VARCHAR, charVal);
        expectedValues.put(ColumnType.CHAR, charVal);
        expectedValues.put(ColumnType.LINK, charVal);
        expectedValues.put(ColumnType.INT, intVal);
        expectedValues.put(ColumnType.INT32, intVal);
        expectedValues.put(ColumnType.BIGINT, Arrays.asList(bigintVal, bigInteger));
        expectedValues.put(ColumnType.DOUBLE, doubleVal);
        expectedValues.put(ColumnType.FLOAT, floatVal);
        expectedValues.put(ColumnType.DATE, dateLongVal.intValue());
        expectedValues.put(ColumnType.TIME, timeLongVal);
        Instant timestampInstant = LocalDateTime.parse(timestampStrVal,
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")).atZone(CoreConstants.CORE_ZONE_ID).toInstant();
        expectedValues.put(ColumnType.TIMESTAMP, timestampInstant.getEpochSecond() * 1000000 + timestampInstant.getNano() / 1000);
        expectedValues.put(ColumnType.BOOLEAN, booleanVal);
        expectedValues.put(ColumnType.UUID, UUID.fromString(uuidStrVal));
        expectedValues.put(ColumnType.ANY, JsonObject.mapFrom(objMapVal));

        assertAll("Varchar converting",
                () -> assertEquals(expectedValues.get(ColumnType.VARCHAR), typeConverter.convert(ColumnType.VARCHAR, charVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.VARCHAR, charVal) instanceof String)
        );
        assertAll("Char converting",
                () -> assertEquals(expectedValues.get(ColumnType.CHAR), typeConverter.convert(ColumnType.CHAR, charVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.CHAR, charVal) instanceof String)
        );
        assertAll("Link converting",
                () -> assertEquals(expectedValues.get(ColumnType.LINK), typeConverter.convert(ColumnType.LINK, charVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.LINK, charVal) instanceof String)
        );
        assertAll("Int converting",
                () -> assertEquals(expectedValues.get(ColumnType.INT), typeConverter.convert(ColumnType.INT, intVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.INT, intVal) instanceof Long)
        );
        assertAll("Int32 converting",
                () -> assertEquals(expectedValues.get(ColumnType.INT32), typeConverter.convert(ColumnType.INT32, intVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.INT32, intVal) instanceof Long)
        );
        assertAll("Bigint converting from long",
                () -> assertEquals(((List) expectedValues.get(ColumnType.BIGINT)).get(0),
                        typeConverter.convert(ColumnType.BIGINT, bigintVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, bigintVal) instanceof Long)
        );
        assertAll("Bigint converting from BigInteger",
                () -> assertEquals(((BigInteger) ((List) expectedValues.get(ColumnType.BIGINT)).get(1)).longValue(),
                        typeConverter.convert(ColumnType.BIGINT, bigInteger)),
                () -> assertTrue(typeConverter.convert(ColumnType.BIGINT, bigInteger) instanceof Long)
        );
        assertAll("Double converting",
                () -> assertEquals(expectedValues.get(ColumnType.DOUBLE), typeConverter.convert(ColumnType.DOUBLE, doubleVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.DOUBLE, doubleVal) instanceof Double)
        );
        assertAll("Float converting",
                () -> assertEquals(expectedValues.get(ColumnType.FLOAT), typeConverter.convert(ColumnType.FLOAT, floatVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.FLOAT, floatVal) instanceof Float)
        );
        assertAll("Date converting",
                () -> assertEquals(expectedValues.get(ColumnType.DATE), typeConverter.convert(ColumnType.DATE, dateLongVal)),
                () -> assertEquals(expectedValues.get(ColumnType.DATE), typeConverter.convert(ColumnType.DATE, dateStrVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.DATE, dateLongVal) instanceof Integer),
                () -> assertTrue(typeConverter.convert(ColumnType.DATE, dateStrVal) instanceof Integer)
        );
        assertAll("Time converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIME), typeConverter.convert(ColumnType.TIME, timeLongVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.TIME, timeLongVal) instanceof Number)
        );
        assertAll("Timestamp converting",
                () -> assertEquals(expectedValues.get(ColumnType.TIMESTAMP), typeConverter.convert(ColumnType.TIMESTAMP,
                        timestampStrVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.TIMESTAMP, timestampStrVal) instanceof Long)
        );
        assertAll("Boolean converting",
                () -> assertEquals(expectedValues.get(ColumnType.BOOLEAN), typeConverter.convert(ColumnType.BOOLEAN, booleanVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.BOOLEAN, booleanVal) instanceof Boolean)
        );
        assertAll("UUID converting",
                () -> assertEquals(expectedValues.get(ColumnType.UUID), typeConverter.convert(ColumnType.UUID, uuidStrVal)),
                () -> assertTrue(typeConverter.convert(ColumnType.UUID, uuidStrVal) instanceof UUID)
        );
        assertAll("Any converting",
                () -> assertEquals(expectedValues.get(ColumnType.ANY), typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal))),
                () -> assertTrue(typeConverter.convert(ColumnType.ANY, JsonObject.mapFrom(objMapVal)) instanceof JsonObject)
        );
    }

    @Test
    void convertWithNull() {
        charVal = null;
        intVal = null;
        bigintVal = null;
        doubleVal = null;
        floatVal = null;
        dateLongVal = null;
        timeLongVal = null;
        timestampLongVal = null;
        booleanVal = null;
        uuidStrVal = null;
        objMapVal = null;
        bigInteger = null;

        assertAll("Varchar converting",
                () -> assertNull(typeConverter.convert(ColumnType.VARCHAR, charVal))

        );
        assertAll("Char converting",
                () -> assertNull(typeConverter.convert(ColumnType.CHAR, charVal))
        );
        assertAll("Link converting",
                () -> assertNull(typeConverter.convert(ColumnType.LINK, charVal))
        );
        assertAll("Int converting",
                () -> assertNull(typeConverter.convert(ColumnType.INT, intVal))
        );
        assertAll("Int32 converting",
                () -> assertNull(typeConverter.convert(ColumnType.INT32, intVal))
        );
        assertAll("Bigint converting from long",
                () -> assertNull(typeConverter.convert(ColumnType.BIGINT, bigintVal))
        );
        assertAll("Bigint converting from bigInteger",
                () -> assertNull(typeConverter.convert(ColumnType.BIGINT, bigInteger))
        );
        assertAll("Double converting",
                () -> assertNull(typeConverter.convert(ColumnType.DOUBLE, doubleVal))
        );
        assertAll("Float converting",
                () -> assertNull(typeConverter.convert(ColumnType.FLOAT, floatVal))
        );
        assertAll("Date converting",
                () -> assertNull(typeConverter.convert(ColumnType.DATE, dateLongVal))
        );
        assertAll("Time converting",
                () -> assertNull(typeConverter.convert(ColumnType.TIME, timeLongVal))
        );
        assertAll("Timestamp converting",
                () -> assertNull(typeConverter.convert(ColumnType.TIMESTAMP, timestampLongVal))
        );
        assertAll("Boolean converting",
                () -> assertNull(typeConverter.convert(ColumnType.BOOLEAN, booleanVal))
        );
        assertAll("UUID converting",
                () -> assertNull(typeConverter.convert(ColumnType.UUID, uuidStrVal))
        );
        assertAll("Any converting",
                () -> assertNull(typeConverter.convert(ColumnType.ANY, objMapVal))
        );
    }
}
