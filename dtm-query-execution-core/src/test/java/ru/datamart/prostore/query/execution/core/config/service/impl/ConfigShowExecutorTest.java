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

import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigCall;
import ru.datamart.prostore.query.execution.core.config.dto.ConfigRequestContext;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import lombok.val;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.boot.env.RandomValuePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigShowExecutorTest {

    @Mock
    private ConfigurableEnvironment configurableEnvironment;

    @InjectMocks
    private ConfigShowExecutor configShowExecutor;

    private MutablePropertySources propertySources;
    private Map<String, String> properties = new HashMap<>();

    @BeforeEach
    void setUp() {
        propertySources = new MutablePropertySources();
        propertySources.addFirst(new OriginTrackedMapPropertySource("test", properties));
        propertySources.addFirst(new RandomValuePropertySource("rnd"));
        when(configurableEnvironment.getPropertySources()).thenReturn(propertySources);
    }

    @Test
    void shouldReturnAllPropertiesWithoutEnvironment() {
        // arrange
        properties.put("group1.group1.val1", "ignored");
        properties.put("group1.group2.val1", "ignored");
        properties.put("val2", "ignored");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW()");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group1:val1"),
                        Matchers.hasEntry("parameter_value", "111"),
                        Matchers.hasEntry("environment_variable", null)
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", null)
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "val2"),
                        Matchers.hasEntry("parameter_value", "2"),
                        Matchers.hasEntry("environment_variable", null)
                )
        ));
    }

    @Test
    void shouldNotDuplicateItemsAndResolveByOrder() {
        // arrange
        properties.put("group1.group1.val1", "ignored");
        properties.put("group1.group2.val1", "ignored");
        properties.put("val2", "${ENV_3}");
        properties.put("val3", "");

        HashMap<String, String> prioritizedProperties = new HashMap<>();
        prioritizedProperties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        prioritizedProperties.put("group1.group2.val1", "${ENV2}");
        prioritizedProperties.put("val2", "");
        propertySources.addFirst(new OriginTrackedMapPropertySource("override", prioritizedProperties));

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");
        when(configurableEnvironment.getProperty("val3")).thenReturn("3");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW()");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group1:val1"),
                        Matchers.hasEntry("parameter_value", "111"),
                        Matchers.hasEntry("environment_variable", "ENV1")
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV2")
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "val2"),
                        Matchers.hasEntry("parameter_value", "2"),
                        Matchers.hasEntry("environment_variable", null)
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "val3"),
                        Matchers.hasEntry("parameter_value", "3"),
                        Matchers.hasEntry("environment_variable", null)
                )
        ));
    }

    @Test
    void shouldReturnAllPropertiesWithEnvironment() {
        // arrange
        properties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        properties.put("group1.group2.val1", "${ENV_2:}");
        properties.put("val2", "${ENV3:with spaces}");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW()");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group1:val1"),
                        Matchers.hasEntry("parameter_value", "111"),
                        Matchers.hasEntry("environment_variable", "ENV1")
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV_2")
                ),
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "val2"),
                        Matchers.hasEntry("parameter_value", "2"),
                        Matchers.hasEntry("environment_variable", "ENV3")
                )
        ));
    }

    @Test
    void shouldReturnPickedPropertyByName() {
        // arrange
        properties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        properties.put("group1.group2.val1", "${ENV_2:}");
        properties.put("val2", "${ENV3:with spaces}");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW('group1:group2:val1')");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV_2")
                )
        ));
    }

    @Test
    void shouldReturnPickedPropertyByNameIgnoreCase() {
        // arrange
        properties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        properties.put("group1.group2.val1", "${ENV_2:}");
        properties.put("val2", "${ENV3:with spaces}");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW('GROUP1:gRoUp2:vAl1')");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV_2")
                )
        ));
    }

    @Test
    void shouldReturnPickedPropertyByEnv() {
        // arrange
        properties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        properties.put("group1.group2.val1", "${ENV_2:}");
        properties.put("val2", "${ENV3:with spaces}");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW('ENV_2')");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV_2")
                )
        ));
    }

    @Test
    void shouldReturnPickedPropertyByEnvIgnoreCase() {
        // arrange
        properties.put("group1.group1.val1", "${ENV1:DEFAULT_VALUE}");
        properties.put("group1.group2.val1", "${ENV_2:}");
        properties.put("val2", "${ENV3:with spaces}");

        when(configurableEnvironment.getProperty("group1.group1.val1")).thenReturn("111");
        when(configurableEnvironment.getProperty("group1.group2.val1")).thenReturn("121");
        when(configurableEnvironment.getProperty("val2")).thenReturn("2");

        val sqlNode = (SqlConfigCall) TestUtils.DEFINITION_SERVICE.processingQuery("CONFIG_SHOW('eNv_2')");
        val context = new ConfigRequestContext(null, null, sqlNode, "env");

        // act
        val call = configShowExecutor.execute(context);

        // assert
        if (call.failed()) {
            fail(call.cause());
        }
        val propsResult = call.result().getResult();
        assertThat(propsResult, containsInAnyOrder(
                Matchers.allOf(
                        Matchers.hasEntry("parameter_name", "group1:group2:val1"),
                        Matchers.hasEntry("parameter_value", "121"),
                        Matchers.hasEntry("environment_variable", "ENV_2")
                )
        ));
    }
}