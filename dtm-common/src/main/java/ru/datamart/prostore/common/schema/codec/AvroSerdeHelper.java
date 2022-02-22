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
package ru.datamart.prostore.common.schema.codec;

import ru.datamart.prostore.common.schema.codec.conversion.BigDecimalConversion;
import ru.datamart.prostore.common.schema.codec.conversion.LocalDateConversion;
import ru.datamart.prostore.common.schema.codec.conversion.LocalDateTimeConversion;
import ru.datamart.prostore.common.schema.codec.conversion.LocalTimeConversion;
import ru.datamart.prostore.common.schema.codec.type.BigDecimalLogicalType;
import ru.datamart.prostore.common.schema.codec.type.LocalDateLogicalType;
import ru.datamart.prostore.common.schema.codec.type.LocalDateTimeLogicalType;
import ru.datamart.prostore.common.schema.codec.type.LocalTimeLogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

public abstract class AvroSerdeHelper {

    protected AvroSerdeHelper() {
    }

    static {
        GenericData.get().addLogicalTypeConversion(LocalDateConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(BigDecimalConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalDateConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalTimeConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        SpecificData.get().addLogicalTypeConversion(BigDecimalConversion.getInstance());
        LogicalTypes.register(BigDecimalLogicalType.INSTANCE.getName(), schema -> BigDecimalLogicalType.INSTANCE);
        LogicalTypes.register(LocalDateTimeLogicalType.INSTANCE.getName(), schema -> LocalDateTimeLogicalType.INSTANCE);
        LogicalTypes.register(LocalDateLogicalType.INSTANCE.getName(), schema -> LocalDateLogicalType.INSTANCE);
        LogicalTypes.register(LocalTimeLogicalType.INSTANCE.getName(), schema -> LocalTimeLogicalType.INSTANCE);
    }
}
