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

package org.apache.flink.table.runtime.datastream

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.datastream.DataStreamAggregateITCase.TimestampWithEqualWatermark
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class DataStreamAggregateITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hallo"),
    (3L, 2, "Hello"),
    (4L, 5, "Hello"),
    (6L, 3, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 4, "Hello world"))

  @Test
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'rowtime as 'w)
      .groupBy('w)
      .select('int.count, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "1,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "1,1970-01-01 00:00:00.014,1970-01-01 00:00:00.019",
      "1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "2,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003",
      "2,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011",
      "3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "4,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingWindowOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 10.milli every 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hello world,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello world,1,1970-01-01 00:00:00.01,1970-01-01 00:00:00.02",
      "Hello world,1,1970-01-01 00:00:00.015,1970-01-01 00:00:00.025",
      "Hello,1,1970-01-01 00:00:00.005,1970-01-01 00:00:00.015",
      "Hello,2,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hello,3,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01",
      "Hi,1,1969-12-31 23:59:59.995,1970-01-01 00:00:00.005",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.01")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingWindowOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 5.milli every 4.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hallo,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello world,1,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hello world,1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013",
      "Hello world,1,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017",
      "Hello world,1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021",
      "Hello,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hello,2,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009",
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCountOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet event-time variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 4.rows every 2.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello world,2", "Hello,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCountOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet event-time variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 6.rows every 1.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hallo,1",
      "Hello world,1",
      "Hello world,2",
      "Hello,1",
      "Hello,2",
      "Hello,3",
      "Hi,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCountNonOverlappingFullPane(): Unit = {
    // please keep this test in sync with the DataSet event-time variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 2.rows every 4.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    assertEquals(Seq(), StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCountNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the DataSet event-time variant
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Slide over 1.rows every 3.rows as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object DataStreamAggregateITCase {
  class TimestampWithEqualWatermark extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
      : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
