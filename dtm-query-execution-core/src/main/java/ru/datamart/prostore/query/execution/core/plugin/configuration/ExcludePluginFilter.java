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
package ru.datamart.prostore.query.execution.core.plugin.configuration;

import ru.datamart.prostore.common.reader.SourceType;
import lombok.val;
import org.springframework.boot.web.reactive.context.AnnotationConfigReactiveWebServerApplicationContext;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExcludePluginFilter implements TypeFilter {
    private static final String PLUGIN_PACKAGE_PATTERN_TEMPLATE = "ru.datamart.prostore.query.execution.plugin.(%s)";
    private static final String CORE_PLUGINS_ACTIVE = "core.plugins.active";
    private Pattern filter;

    @Override
    public boolean match(MetadataReader metadataReader,
                         MetadataReaderFactory metadataReaderFactory) {

        if (filter == null) {
            filter = initFilter(metadataReaderFactory).orElse(Pattern.compile(""));
        }
        val classMetadata = metadataReader.getClassMetadata();
        val fullyQualifiedName = classMetadata.getClassName();
        return filter.matcher(fullyQualifiedName).matches();
    }

    private Optional<Pattern> initFilter(MetadataReaderFactory metadataReaderFactory) {
        val activePlugins = getActivePlugins(metadataReaderFactory);
        val excludedSources = getExcludedSources(activePlugins);
        if (activePlugins.isEmpty() || excludedSources.isEmpty()) {
            return Optional.empty();
        } else {
            val filterPattern = String.format(PLUGIN_PACKAGE_PATTERN_TEMPLATE, String.join("|", excludedSources));
            return Optional.of(Pattern.compile(filterPattern));
        }
    }

    private List<String> getExcludedSources(List<String> activePlugins) {
        return Arrays.stream(SourceType.values())
                .filter(sourceType -> sourceType != SourceType.INFORMATION_SCHEMA)
                .map(Enum::name)
                .filter(typeName -> activePlugins.stream()
                        .noneMatch(apName -> apName.equalsIgnoreCase(typeName)))
                .map(typeName -> String.format("%s.*", typeName.toLowerCase()))
                .collect(Collectors.toList());
    }

    private List<String> getActivePlugins(MetadataReaderFactory metadataReaderFactory) {
        return Arrays.stream(getActivePluginsSettings(metadataReaderFactory).split(","))
                .map(pluginName -> pluginName.toUpperCase().trim())
                .collect(Collectors.toList());
    }

    private String getActivePluginsSettings(MetadataReaderFactory metadataReaderFactory) {
        val cachingMetadataReaderFactory = (CachingMetadataReaderFactory) metadataReaderFactory;
        val applicationContext = (AnnotationConfigReactiveWebServerApplicationContext) cachingMetadataReaderFactory.getResourceLoader();
        val environment = applicationContext.getEnvironment();
        return environment.getProperty(CORE_PLUGINS_ACTIVE);
    }
}
