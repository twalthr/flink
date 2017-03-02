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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

trait DataSetRel extends RelNode with FlinkRelNode {

  /**
    * Translates the [[DataSetRel]] node into a [[DataSet]] operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    * @return DataSet of type [[Row]]
    */
  def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row]

  def getPhysicalFieldNames: Seq[String] = {
    getRowType.getFieldNames
  }

  def getPhysicalRowType: TypeInformation[Row] = {
    FlinkTypeFactory.toInternalRowTypeInfo(getRowType)
  }
}
