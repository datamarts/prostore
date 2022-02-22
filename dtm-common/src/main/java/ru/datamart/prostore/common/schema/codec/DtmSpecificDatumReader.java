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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

public class DtmSpecificDatumReader<T> extends SpecificDatumReader<T> {
    public DtmSpecificDatumReader() {
    }

    public DtmSpecificDatumReader(Class<T> c) {
        super(c);
    }

    public DtmSpecificDatumReader(Schema schema) {
        super(schema);
    }

    public DtmSpecificDatumReader(Schema writer, Schema reader) {
        super(writer, reader);
    }

    public DtmSpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
        super(writer, reader, data);
    }

    public DtmSpecificDatumReader(SpecificData data) {
        super(data);
    }

    @Override
    protected Class findStringClass(Schema schema) {
        return String.class;
    }
}
