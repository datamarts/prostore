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
package ru.datamart.prostore.query.execution.plugin.adqm.base.utils;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.execution.plugin.adqm.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import io.vertx.core.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class AdqmDdlUtil {
    public static final String NULLABLE_FIELD = "%s Nullable(%s)";
    public static final String NOT_NULLABLE_FIELD = "%s %s";
    private static final String STRING_TYPE = "String";
    private static final String INT_32_TYPE = "Int32";
    private static final String INT_64_TYPE = "Int64";
    private static final String UINT_8_TYPE = "UInt8";
    private static final String FLOAT_32_TYPE = "Float32";
    private static final String FLOAT_64_TYPE = "Float64";

    private AdqmDdlUtil() {
    }

    public static Optional<String> validateRequest(MppwRequest request) {
        if (request == null) {
            return Optional.of("MppwRequest should not be null");
        }

        if (request.getUploadMetadata().getExternalSchema() == null) {
            return Optional.of("MppwRequest.schema should not be null");
        }

        return Optional.empty();
    }

    public static String getQualifiedTableName(@NonNull MppwRequest request) {

        String tableName = request.getDestinationEntity().getName();
        String schema = request.getDatamartMnemonic();
        String env = request.getEnvName();
        return env + "__" + schema + "." + tableName;
    }

    public static Optional<Pair<String, String>> splitQualifiedTableName(@NonNull String table) {
        String[] parts = table.split("\\.");
        if (parts.length != 2) {
            return Optional.empty();
        }

        return Optional.of(Pair.of(parts[0], parts[1]));
    }

    public static String classTypeToNative(@NonNull ColumnType type) {
        switch (type) {
            case UUID:
            case ANY:
            case CHAR:
            case LINK:
            case VARCHAR:
                return STRING_TYPE;
            case INT32:
                return INT_32_TYPE;
            case INT:
            case BIGINT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return INT_64_TYPE;
            case BOOLEAN:
                return UINT_8_TYPE;
            case FLOAT:
                return FLOAT_32_TYPE;
            case DOUBLE:
                return FLOAT_64_TYPE;
            default:
                return "";
        }
    }

    public static String avroTypeToNative(@NonNull Schema f) {
        // we support UNION schema (with nullable option) and primitive type schemas
        switch (f.getType()) {
            case UNION:
                val fields = f.getTypes();
                val types = fields.stream().map(AdqmDdlUtil::avroTypeToNative).collect(Collectors.toList());
                if (types.size() == 2) { // We support only union (null, type)
                    int realTypeIdx = types.get(0).equalsIgnoreCase("NULL") ? 1 : 0;
                    return avroTypeToNative(fields.get(realTypeIdx));
                } else {
                    return "";
                }
            case STRING:
                return STRING_TYPE;
            case INT:
                if (f.getLogicalType() instanceof LogicalTypes.Date) {
                    return INT_64_TYPE;
                }
                return INT_32_TYPE;
            case LONG:
                return INT_64_TYPE;
            case FLOAT:
                return FLOAT_32_TYPE;
            case DOUBLE:
                return FLOAT_64_TYPE;
            case BOOLEAN:
                return UINT_8_TYPE;
            case NULL:
                return "NULL";
            default:
                return "";
        }
    }

    public static String avroFieldToString(@NonNull Schema.Field f) {
        return avroFieldToString(f, true);
    }

    public static String avroFieldToString(@NonNull Schema.Field f, boolean isNullable) {
        String name = f.name();
        String type = avroTypeToNative(f.schema());
        String template = isNullable ? NULLABLE_FIELD : NOT_NULLABLE_FIELD;

        return String.format(template, name, type);
    }

    public static <T, E> Future<T> sequenceAll(@NonNull final List<E> actions,
                                               @NonNull final Function<E, Future<T>> action) {
        try {
            Future<T> result = null;
            for (E a : actions) {
                if (result == null) {
                    result = action.apply(a);
                } else {
                    result = result.compose(v -> action.apply(a));
                }
            }
            return result == null ? Future.succeededFuture() : result;
        } catch (Exception e) {
            log.error("Error sequence executing for actions: {}", actions);
            return Future.failedFuture(e);
        }
    }
}
