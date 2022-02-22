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
package ru.datamart.prostore.query.execution.core.config.service.impl;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigType;
import ru.datamart.prostore.query.calcite.core.extension.config.function.SqlConfigShow;
import ru.datamart.prostore.query.execution.core.config.dto.ConfigRequestContext;
import ru.datamart.prostore.query.execution.core.config.service.ConfigExecutor;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConfigShowExecutor implements ConfigExecutor {
    private static final Pattern ENVIRONMENT_PATTERN = Pattern.compile("\\$\\{(\\w+)(:.*)?}");
    public static final String PARAMETER_NAME_COLUMN = "parameter_name";
    public static final String PARAMETER_VALUE_COLUMN = "parameter_value";
    public static final String ENVIRONMENT_VARIABLE_COLUMN = "environment_variable";
    private static final List<ColumnMetadata> METADATA = Arrays.asList(
            ColumnMetadata.builder().name(PARAMETER_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(PARAMETER_VALUE_COLUMN).type(ColumnType.VARCHAR).build(),
            ColumnMetadata.builder().name(ENVIRONMENT_VARIABLE_COLUMN).type(ColumnType.VARCHAR).build()
    );
    private final ConfigurableEnvironment environment;

    public ConfigShowExecutor(ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        return Future.future(event -> {
            val sqlNode = (SqlConfigShow) context.getSqlNode();
            val properties = getProperties();
            val foundProperties = getAllOrSelected(properties, sqlNode.getParameterName());
            val result = foundProperties.stream().map(this::mapToResult).collect(Collectors.toList());
            event.complete(QueryResult.builder().result(result).metadata(METADATA).build());
        });
    }

    private List<Property> getAllOrSelected(List<Property> properties, SqlCharStringLiteral parameterName) {
        if (parameterName == null || parameterName.getNlsString() == null ||
                parameterName.getNlsString().getValue() == null) {
            return properties;
        }

        val parameterToFind = parameterName.getNlsString().getValue().toLowerCase(Locale.ROOT);
        return properties.stream()
                .filter(property -> hasEqualParameterNameOrEnv(parameterToFind, property))
                .collect(Collectors.toList());
    }

    private List<Property> getProperties() {
        Map<String, Property> result = new LinkedHashMap<>();
        for (PropertySource<?> propertySource : environment.getPropertySources()) {
            if (propertySource instanceof OriginTrackedMapPropertySource) {
                ((OriginTrackedMapPropertySource) propertySource).getSource().forEach((key, value) -> {
                    val parameterName = key.replace(".", ":");
                    val environmentName = extractEnvironmentVariable(String.valueOf(value));
                    val resolvedValue = environment.getProperty(key);
                    result.putIfAbsent(parameterName, new Property(parameterName, environmentName, resolvedValue));
                });
            }
        }
        return new ArrayList<>(result.values());
    }

    private String extractEnvironmentVariable(String value) {
        val matcher = ENVIRONMENT_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }

        return matcher.group(1);
    }

    private boolean hasEqualParameterNameOrEnv(String parameterToFind, Property property) {
        return parameterToFind.equalsIgnoreCase(property.parameterName) || parameterToFind.equalsIgnoreCase(property.environmentName);
    }

    private Map<String, Object> mapToResult(Property property) {
        Map<String, Object> result = new HashMap<>();
        result.put(PARAMETER_NAME_COLUMN, property.parameterName);
        result.put(PARAMETER_VALUE_COLUMN, property.resolvedValue);
        result.put(ENVIRONMENT_VARIABLE_COLUMN, property.environmentName);
        return result;
    }

    @Override
    public SqlConfigType getConfigType() {
        return SqlConfigType.CONFIG_SHOW;
    }

    @AllArgsConstructor
    private static class Property {
        private final String parameterName;
        private final String environmentName;
        private final String resolvedValue;
    }
}
