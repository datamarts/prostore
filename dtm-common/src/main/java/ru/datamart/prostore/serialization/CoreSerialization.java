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
package ru.datamart.prostore.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.val;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.exception.DtmException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class CoreSerialization {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        val objectMapper = DatabindCodec.mapper().copy();
        val simpleModule = new SimpleModule();
        objectMapper.registerModule(simpleModule);
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER = objectMapper;
    }

    private CoreSerialization() {
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't deserialize bytes [%s]", clazz.getName()), e);
        }
    }

    public static <T> T deserialize(String string, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(string, clazz);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't deserialize string [%s]", clazz.getName()), e);
        }
    }

    public static byte[] serialize(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't serialize as bytes [%s]", classOf(obj)), e);
        }
    }

    public static String serializeAsString(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't serialize as string [%s]", classOf(obj)), e);
        }
    }

    public static JsonNode readTree(String string) {
        try {
            return OBJECT_MAPPER.readTree(string);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't read tree from string [%s]", string), e);
        }
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
    }

    private static String classOf(Object obj) {
        if (obj == null) {
            return "null";
        }

        return obj.getClass().getName();
    }

    public static ObjectMapper mapper() {
        return OBJECT_MAPPER;
    }
}
