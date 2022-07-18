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

package org.apache.flink.table.runtime.operators.dpp;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** DynamicFilteringDataCollectorOperator. */
public class DynamicFilteringDataCollectorOperator extends AbstractStreamOperator<Object>
        implements OneInputStreamOperator<RowData, Object> {

    private final RowType dynamicFilteringFieldType;
    private final List<Integer> dynamicFilteringFieldIndices;
    private final long threshold;
    private transient long currentSize;
    /** Use Set instead of List to ignore the duplicated records. */
    private transient Set<byte[]> buffer;

    private transient TypeInformation<RowData> typeInfo;
    private transient TypeSerializer<RowData> serializer;
    private transient boolean eventSend;

    private final OperatorEventGateway operatorEventGateway;

    public DynamicFilteringDataCollectorOperator(
            RowType dynamicFilteringFieldType,
            List<Integer> dynamicFilteringFieldIndices,
            long threshold,
            OperatorEventGateway operatorEventGateway) {
        this.dynamicFilteringFieldType = dynamicFilteringFieldType;
        this.dynamicFilteringFieldIndices = dynamicFilteringFieldIndices;
        this.threshold = threshold;
        this.operatorEventGateway = operatorEventGateway;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.buffer = new HashSet<>();
        this.currentSize = 0L;
        this.typeInfo = InternalTypeInfo.of(dynamicFilteringFieldType);
        this.serializer = typeInfo.createSerializer(new ExecutionConfig());
        this.eventSend = false;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (currentSize > threshold) {
            return;
        }

        RowData value = element.getValue();
        GenericRowData rowData = new GenericRowData(dynamicFilteringFieldIndices.size());
        for (int i = 0; i < dynamicFilteringFieldIndices.size(); ++i) {
            LogicalType type = dynamicFilteringFieldType.getTypeAt(i);
            int index = dynamicFilteringFieldIndices.get(i);
            switch (type.getTypeRoot()) {
                case INTEGER:
                    rowData.setField(i, value.getInt(index));
                    break;
                case BIGINT:
                    rowData.setField(i, value.getLong(index));
                    break;
                case VARCHAR:
                    rowData.setField(i, value.getString(index));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(value, wrapper);
        boolean duplicated = !buffer.add(baos.toByteArray());
        if (duplicated) {
            return;
        }
        currentSize += baos.size();
        if (exceedThreshold()) {
            sendEvent();
        }
    }

    private boolean exceedThreshold() {
        return threshold > 0 && currentSize > threshold;
    }

    public void finish() throws Exception {
        sendEvent();
    }

    private void sendEvent() {
        if (eventSend) {
            return;
        }

        final DynamicFilteringData dynamicFilteringData;
        if (exceedThreshold()) {
            dynamicFilteringData =
                    new DynamicFilteringData(
                            typeInfo, dynamicFilteringFieldType, new ArrayList<>(), true);
        } else {
            dynamicFilteringData =
                    new DynamicFilteringData(
                            typeInfo, dynamicFilteringFieldType, new ArrayList<>(buffer), false);
        }

        DynamicFilteringEvent event = new DynamicFilteringEvent(dynamicFilteringData);
        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
        this.eventSend = true;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (buffer != null) {
            buffer.clear();
        }
    }
}
