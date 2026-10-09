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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ProcessSetTableOperator} notifies stateful sets for a table with broadcast
 * semantics.
 *
 * <p>A minimal {@link ProcessTableRunner} is used that counts rows of the main table per set. A
 * broadcast row updates a broadcast rule. If notified, every stateful set emits its count together
 * with the rule and registers a timer. The rule "clear" clears the set's state.
 */
class ProcessSetTableOperatorNotifyStatefulSetsTest {

    private static final DataType MAIN_TYPE =
            DataTypes.ROW(DataTypes.FIELD("k", DataTypes.BIGINT()));

    private static final DataType BROADCAST_TYPE =
            DataTypes.ROW(DataTypes.FIELD("rule", DataTypes.STRING()));

    // Output layout is driven by PassPartitionKeysCollector / PassAllCollector in
    // AbstractProcessTableOperator: [partition-key, PTF-emitted label].
    private static final DataType OUTPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.BIGINT()),
                    DataTypes.FIELD("label", DataTypes.STRING()));

    private static final InternalTypeInfo<RowData> MAIN_TYPE_INFO =
            InternalTypeInfo.of(MAIN_TYPE.getLogicalType());

    private static final InternalTypeInfo<RowData> BROADCAST_TYPE_INFO =
            InternalTypeInfo.of(BROADCAST_TYPE.getLogicalType());

    private static final InternalTypeInfo<RowData> OUTPUT_TYPE_INFO =
            InternalTypeInfo.of(OUTPUT_TYPE.getLogicalType());

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, MAIN_TYPE_INFO.toRowFieldTypes());

    private static final long TIMER_TIME = 100L;

    @Test
    void testNotifyStatefulSets() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness = createHarness(true)) {
            processMainRows(harness);
            // Notifies sets 1 and 2
            processBroadcastRow(harness, "a");
            // Notifies sets 1 and 2, which clear their state while iterating over the keys
            processBroadcastRow(harness, "clear");
            // No stateful sets left
            processBroadcastRow(harness, "b");
            // Fires timers registered during notification
            processWatermarks(harness);

            final List<String> output = describeOutput(harness);
            assertThat(output).hasSize(9);
            assertThat(output.subList(0, 3)).containsExactly("1:count-1", "2:count-1", "1:count-2");
            // The order of sets is defined by the state backend
            assertThat(output.subList(3, 5))
                    .containsExactlyInAnyOrder(
                            "1:notified-2-a-read-only", "2:notified-1-a-read-only");
            assertThat(output.subList(5, 7))
                    .containsExactlyInAnyOrder(
                            "1:notified-2-clear-read-only", "2:notified-1-clear-read-only");
            assertThat(output.subList(7, 9)).containsExactlyInAnyOrder("1:fired", "2:fired");
        }
    }

    @Test
    void testWithoutNotifyStatefulSets() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness = createHarness(false)) {
            processMainRows(harness);
            processBroadcastRow(harness, "a");
            processWatermarks(harness);

            assertThat(describeOutput(harness))
                    .containsExactly("1:count-1", "2:count-1", "1:count-2");
        }
    }

    private static void processMainRows(StreamTaskMailboxTestHarness<RowData> harness)
            throws Exception {
        harness.processElement(new StreamRecord<>(GenericRowData.of(1L)), 0);
        harness.processElement(new StreamRecord<>(GenericRowData.of(2L)), 0);
        harness.processElement(new StreamRecord<>(GenericRowData.of(1L)), 0);
    }

    private static void processBroadcastRow(
            StreamTaskMailboxTestHarness<RowData> harness, String rule) throws Exception {
        harness.processElement(
                new StreamRecord<>(GenericRowData.of(StringData.fromString(rule))), 1);
    }

    private static void processWatermarks(StreamTaskMailboxTestHarness<RowData> harness)
            throws Exception {
        harness.processElement(new Watermark(TIMER_TIME), 0);
        harness.processElement(new Watermark(TIMER_TIME), 1);
        harness.processAll();
    }

    private static List<String> describeOutput(StreamTaskMailboxTestHarness<RowData> harness) {
        return harness.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(r -> (RowData) ((StreamRecord<?>) r).getValue())
                .map(r -> r.getLong(0) + ":" + r.getString(1))
                .collect(Collectors.toList());
    }

    private StreamTaskMailboxTestHarness<RowData> createHarness(boolean notifyStatefulSets)
            throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        MultipleInputStreamTask::new, OUTPUT_TYPE_INFO)
                .setKeyType(KEY_SELECTOR.getProducedType())
                .addInput(MAIN_TYPE_INFO, 1, KEY_SELECTOR)
                .addInput(BROADCAST_TYPE_INFO, 1, null)
                .setupOperatorChain(new TestOperatorFactory(notifyStatefulSets))
                .name("ptf")
                .finishForSingletonOperatorChain(
                        OUTPUT_TYPE_INFO.createSerializer(new SerializerConfigImpl()))
                .build();
    }

    private static List<RuntimeTableSemantics> tableSemantics(boolean notifyStatefulSets) {
        return List.of(
                new RuntimeTableSemantics(
                        "main",
                        0,
                        MAIN_TYPE,
                        new int[] {0},
                        new int[0],
                        new RuntimeTableSemantics.SortDirection[0],
                        RuntimeChangelogMode.serialize(ChangelogMode.insertOnly()),
                        /* passColumnsThrough */ false,
                        /* hasSetSemantics */ true,
                        /* hasBroadcastSemantics */ false,
                        /* notifiesStatefulSets */ false,
                        /* timeColumn */ -1,
                        /* upsertKeyColumns */ List.of()),
                new RuntimeTableSemantics(
                        "broadcast",
                        1,
                        BROADCAST_TYPE,
                        new int[0],
                        new int[0],
                        new RuntimeTableSemantics.SortDirection[0],
                        RuntimeChangelogMode.serialize(ChangelogMode.insertOnly()),
                        /* passColumnsThrough */ false,
                        /* hasSetSemantics */ false,
                        /* hasBroadcastSemantics */ true,
                        notifyStatefulSets,
                        /* timeColumn */ -1,
                        /* upsertKeyColumns */ List.of()));
    }

    private static List<RuntimeStateInfo> stateInfos() {
        return List.of(
                new RuntimeStateInfo(
                        "count", ValueView.newValueViewDataType(DataTypes.INT()), 0, false),
                new RuntimeStateInfo(
                        "rule", ValueView.newValueViewDataType(DataTypes.STRING()), 0, true));
    }

    // --------------------------------------------------------------------------------------------
    // Factory and runner
    // --------------------------------------------------------------------------------------------

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<RowData> {
        private static final long serialVersionUID = 1L;

        private final boolean notifyStatefulSets;

        TestOperatorFactory(boolean notifyStatefulSets) {
            this.notifyStatefulSets = notifyStatefulSets;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            return (T)
                    new ProcessSetTableOperator(
                            parameters,
                            tableSemantics(notifyStatefulSets),
                            stateInfos(),
                            new RecordComparator[2],
                            new TestProcessTableRunner(),
                            new HashFunction[2],
                            new RecordEqualiser[2],
                            RuntimeChangelogMode.serialize(ChangelogMode.insertOnly()),
                            List.of());
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return ProcessSetTableOperator.class;
        }
    }

    /**
     * Minimal {@link ProcessTableRunner} that accesses the state handles directly instead of
     * converting state like generated code does.
     */
    private static class TestProcessTableRunner extends ProcessTableRunner {

        @Override
        @SuppressWarnings("unchecked")
        public void callEval() throws Exception {
            final ValueState<Integer> count = (ValueState<Integer>) stateHandles[0].getState();
            final ValueState<String> rule = (ValueState<String>) stateHandles[1].getState();
            if (inputIndex == 0) {
                final int c = Optional.ofNullable(count.value()).orElse(0) + 1;
                count.update(c);
                emit("count-" + c);
            } else if (isProcessingBroadcast()) {
                rule.update(inputRow.getString(0).toString());
            } else {
                String label = "notified-" + count.value() + "-" + rule.value();
                try {
                    rule.update("invalid");
                } catch (TableRuntimeException e) {
                    label += "-read-only";
                }
                emit(label);
                runnerContext.timeContext(Long.class).registerOnTime(TIMER_TIME);
                if (rule.value().equals("clear")) {
                    count.clear();
                }
            }
        }

        @Override
        public void callOnTimer() {
            onTimerCollector.collect(GenericRowData.of(StringData.fromString("fired")));
        }

        private void emit(String label) {
            evalCollector.collect(GenericRowData.of(StringData.fromString(label)));
        }
    }
}
