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

package org.apache.flink.table.runtime.operators.dyamicfiltering;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;

import java.util.Arrays;
import java.util.List;

/** This is the placeholder operator right. */
public class DynamicFilteringPlaceholderOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {

    public DynamicFilteringPlaceholderOperator(StreamOperatorParameters<RowData> parameters) {
        super(parameters, 2);
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new DynamicFilteringDataCollectorInput(), new FactTableSourceInput(output));
    }

    /** FactTableSourceInput. */
    public static class FactTableSourceInput implements Input<RowData> {
        private final Output<StreamRecord<RowData>> output;;

        public FactTableSourceInput(Output<StreamRecord<RowData>> output) {
            this.output = output;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            output.emitWatermark(mark);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            output.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void setKeyContextElement(StreamRecord<RowData> record) throws Exception {}
    }

    /** DynamicFilteringDataCollectorInput. */
    public static class DynamicFilteringDataCollectorInput implements Input<Object> {

        @Override
        public void processElement(StreamRecord<Object> element) throws Exception {}

        @Override
        public void processWatermark(Watermark mark) throws Exception {}

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {}

        @Override
        public void setKeyContextElement(StreamRecord<Object> record) throws Exception {}
    }
}
