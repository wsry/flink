/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.connector.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/** DynamicFilteringData. */
public class DynamicFilteringData implements Serializable {
    private final TypeInformation<RowData> typeInfo;
    private final RowType rowType;
    private final List<byte[]> serializedData;
    private final boolean exceedThreshold;
    private transient List<RowData> data;

    public DynamicFilteringData(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            List<byte[]> serializedData,
            boolean exceedThreshold) {
        this.typeInfo = typeInfo;
        this.rowType = rowType;
        this.serializedData = serializedData;
        this.exceedThreshold = exceedThreshold;
    }

    public RowType getRowType() {
        return rowType;
    }

    public Optional<List<RowData>> getData() {
        if (exceedThreshold) {
            return Optional.empty();
        }

        if (data == null) {
            data = new ArrayList<>();
            TypeSerializer<RowData> serializer = typeInfo.createSerializer(new ExecutionConfig());
            for (byte[] bytes : serializedData) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais)) {
                    RowData partition = serializer.deserialize(inView);
                    data.add(partition);
                } catch (Exception e) {
                    throw new TableException("Unable to deserialize the value.", e);
                }
            }
        }
        return Optional.of(data);
    }

    public boolean contains(RowData row) {
        Optional<List<RowData>> dataOpt = getData();
        if (!dataOpt.isPresent()) {
            return true;
        } else if (row.getArity() != rowType.getFieldCount()) {
            throw new TableException("The arity of RowData is different");
        } else {
            List<RowData> data = dataOpt.get();
            RowData.FieldGetter[] fieldGetters =
                    IntStream.range(0, rowType.getFieldCount())
                            .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                            .toArray(RowData.FieldGetter[]::new);
            for (RowData rd : data) {
                boolean equals = true;
                for (int i = 0; i < rowType.getFieldCount(); ++i) {
                    if (!Objects.equals(
                            fieldGetters[i].getFieldOrNull(row),
                            fieldGetters[i].getFieldOrNull(rd))) {
                        equals = false;
                        break;
                    }
                }
                if (equals) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public String toString() {
        Optional<List<RowData>> data = getData();
        return data.map(p -> "DynamicFilteringData{data=" + p + '}')
                .orElse("DynamicFilteringData{exceedThreshold=true}");
    }
}
