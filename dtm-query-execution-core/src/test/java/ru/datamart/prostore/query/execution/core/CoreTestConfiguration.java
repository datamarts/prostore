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
package ru.datamart.prostore.query.execution.core;

import ru.datamart.prostore.query.execution.core.base.utils.BeanNameGenerator;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;

/**
 * Отделяем основной контекст CORE от плагинов делая его независимым для тестирования
 */
@Profile("test")
@EnableAutoConfiguration(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ComponentScan(basePackages = {
        "ru.datamart.prostore.query.execution.core.calcite",
        "ru.datamart.prostore.query.execution.core.configuration",
        "ru.datamart.prostore.query.execution.core.converter",
        "ru.datamart.prostore.query.execution.core.dao",
        "ru.datamart.prostore.query.execution.core.factory",
        "ru.datamart.prostore.query.execution.core.registry",
        "ru.datamart.prostore.query.execution.core.service",
        "ru.datamart.prostore.query.execution.core.transformer",
        "ru.datamart.prostore.query.execution.core.utils",
        "ru.datamart.prostore.kafka.core.configuration",
        "ru.datamart.prostore.kafka.core.repository",
        "ru.datamart.prostore.kafka.core.service.kafka"}, nameGenerator = BeanNameGenerator.class)
public class CoreTestConfiguration {
}
