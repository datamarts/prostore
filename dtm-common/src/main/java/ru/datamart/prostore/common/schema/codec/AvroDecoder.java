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
package ru.datamart.prostore.common.schema.codec;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AvroDecoder extends AvroSerdeHelper {
    private final DtmSpecificDatumReader<GenericRecord> datumReader = new DtmSpecificDatumReader<>(SpecificData.get());

    @SneakyThrows
    public List<GenericRecord> decode(byte[] encodedData) {
        val values = new ArrayList<GenericRecord>();
        try (val sin = new SeekableByteArrayInput(encodedData)) {
            try (val reader = new DataFileReader<>(sin, datumReader)) {
                while (reader.hasNext()) {
                    values.add(reader.next());
                }
            }
        } catch (Exception e) {
            log.error("AVRO deserialization error", e);
            throw e;
        }
        return values;
    }
}
