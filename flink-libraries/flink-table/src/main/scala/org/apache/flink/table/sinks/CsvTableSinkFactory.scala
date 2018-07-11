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

package org.apache.flink.table.sinks

import java.util

import org.apache.flink.table.api.TableException
import org.apache.flink.table.factories.{TableFactory, TableSinkFactory}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator._
import org.apache.flink.table.descriptors.CsvValidator._
import org.apache.flink.table.descriptors.DescriptorProperties._
import org.apache.flink.table.descriptors.FileSystemValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator._
import org.apache.flink.table.descriptors.SchemaValidator._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

/**
  * Factory for creating configured instances of [[CsvTableSink]].
  */
class CsvTableSinkFactory extends TableSinkFactory[Row] with TableFactory {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE)
    context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context.put(FORMAT_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    // connector
    properties.add(CONNECTOR_PATH)
    // format
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.TYPE}")
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.NAME}")
    properties.add(FORMAT_FIELD_DELIMITER)
    properties.add(CONNECTOR_PATH)
    // schema
    properties.add(s"$SCHEMA.#.${DescriptorProperties.TYPE}")
    properties.add(s"$SCHEMA.#.${DescriptorProperties.NAME}")
    properties
  }

  override def createTableSink(properties: util.Map[String, String]): TableSink[Row] = {
    val params = new DescriptorProperties()
    params.putProperties(properties)

    // validate
    new FileSystemValidator().validate(params)
    new CsvValidator().validate(params)
    new SchemaValidator(true, false, false).validate(params)

    // build
    val formatSchema = params.getTableSchema(FORMAT_FIELDS)
    val tableSchema = SchemaValidator.deriveTableSinkSchema(params)

    if (!formatSchema.equals(tableSchema)) {
      throw new TableException(
        "Encodings that differ from the schema are not supported yet for CsvTableSink.")
    }

    val path = params.getString(CONNECTOR_PATH)
    val fieldDelimiter = toScala(params.getOptionalString(FORMAT_FIELD_DELIMITER)).getOrElse(",")

    val csvTableSink = new CsvTableSink(path, fieldDelimiter)

    csvTableSink
      .configure(formatSchema.getColumnNames, formatSchema.getTypes)
      .asInstanceOf[CsvTableSink]
  }
}
