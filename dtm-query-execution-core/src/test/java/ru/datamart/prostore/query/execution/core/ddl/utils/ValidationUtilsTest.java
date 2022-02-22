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
package ru.datamart.prostore.query.execution.core.ddl.utils;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValidationUtilsTest {
    private static final String CORRECT_COL_001_NAME_LC = "col_001";
    private static final String CORRECT_COL_002_NAME_LC = "s_002_col";
    private static final String CORRECT_ENTITY_NAME_LC = "name_001";
    private static final String CORRECT_SCHEMA_NAME_LC = "s_001_schema";

    private static final String CORRECT_COL_001_NAME_UC = "COL_001";
    private static final String CORRECT_COL_002_NAME_UC = "S_002_COL";
    private static final String CORRECT_ENTITY_NAME_UC = "NAME_001";
    private static final String CORRECT_SCHEMA_NAME_UC = "S_001_SCHEMA";


    @Test
    void shouldSuccessWhenCorrectEntity() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        ValidationUtils.checkEntityNames(entity);
    }

    @Test
    void shouldSuccessWhenCorrectEntityUpperCase() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_UC)
                .schema(CORRECT_SCHEMA_NAME_UC)
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_UC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_UC).build()
                ))
                .build();

        // act assert
        ValidationUtils.checkEntityNames(entity);
    }

    @Test
    void shouldSuccessWhenCorrectName() {
        // arrange
        val name = "name001_";

        // act assert
        ValidationUtils.checkName(name);
    }

    @Test
    void shouldSuccessWhenCorrectNameUpperCase() {
        // arrange
        val name = "NAME001_";

        // act assert
        ValidationUtils.checkName(name);
    }

    @Test
    void shouldSuccessWhenNoFields() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_UC)
                .schema(CORRECT_SCHEMA_NAME_UC)
                .build();

        // act assert
        ValidationUtils.checkEntityNames(entity);
    }

    @Test
    void shouldFailWhenEntityNameStartedByNumber() {
        // arrange
        val entity = Entity.builder()
                .name("001_name")
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [s_001_schema.001_name] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenEntityNameContainsNonLatin() {
        // arrange
        val entity = Entity.builder()
                .name("имя_")
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [s_001_schema.имя_] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenSchemaStartedByNumber() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema("001schema")
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [001schema.name_001] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenSchemaContainsNonLatin() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema("schemaСхема001_")
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [schemaСхема001_.name_001] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenFieldNameStartedByNumber() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name("001_col").build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity columns [001_col] name is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenFieldNameContainsNonLatin() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name("col_001айди").build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity columns [col_001айди] name is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenMultipleFieldsNotValid() {
        // arrange
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name("_col001айди").build(),
                        EntityField.builder().name("0count").build()
                ))
                .build();

        // act assert
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity columns [_col001айди, 0count] name is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenNameStartedByNumber() {
        // arrange
        val name = "001_name";

        // act
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkName(name));
        assertEquals("Identifier [001_name] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenNameStartsWithUnderscore() {
        // arrange
        val name = "_001_name";

        // act
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkName(name));
        assertEquals("Identifier [_001_name] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenNameContainsNonLatin() {
        // arrange
        val name = "name_001имя";

        // act
        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkName(name));
        assertEquals("Identifier [name_001имя] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenEntityNameStartsWithUnderscore() {
        val entity = Entity.builder()
                .name("_name")
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [s_001_schema._name] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenSchemaStartsWithUnderscore() {
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema("_schema")
                .fields(Arrays.asList(
                        EntityField.builder().name(CORRECT_COL_001_NAME_LC).build(),
                        EntityField.builder().name(CORRECT_COL_002_NAME_LC).build()
                ))
                .build();

        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity name [_schema.name_001] is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

    @Test
    void shouldFailWhenFieldStartsWithUnderscore() {
        val entity = Entity.builder()
                .name(CORRECT_ENTITY_NAME_LC)
                .schema(CORRECT_SCHEMA_NAME_LC)
                .fields(Arrays.asList(
                        EntityField.builder().name("_col_1").build(),
                        EntityField.builder().name("_col_2").build()
                ))
                .build();

        val validationDtmException = assertThrows(ValidationDtmException.class, () -> ValidationUtils.checkEntityNames(entity));
        assertEquals("Entity columns [_col_1, _col_2] name is not valid, allowed pattern is [a-zA-Z][a-zA-Z0-9_]*", validationDtmException.getMessage());
    }

}