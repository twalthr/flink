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

package org.apache.flink.table.connectors

import javax.annotation.Nullable

import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

/**
  * Extends a [[TableSource]] or [[TableSink]] to specify a processing time attribute.
  */
trait DefinedProctimeAttribute {

  /**
    * Returns the name of a processing time attribute or null if no processing time attribute is
    * present.
    *
    * The referenced attribute must be present in the [[TableSchema]] of the [[TableSource]]
    * or [[TableSink]] and of type [[Types.SQL_TIMESTAMP]].
    */
  @Nullable
  def getProctimeAttribute: String
}
