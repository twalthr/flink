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

package org.apache.flink.api.table.codegen.calls

import java.lang.reflect.Method

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rex.RexCall
import org.apache.calcite.sql.`type`.SqlTypeName.TIMESTAMP
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO}
import org.apache.flink.api.table.codegen.calls.CallGenerator._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedExpression}

class DateTimeFloorCeilCallGen(
      method: Method,
      timestampMethod: Method,
      dateMethod: Method) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression],
      call: RexCall)
    : GeneratedExpression = {
    val returnType = getLogicalReturnType(call) match {
      case TIMESTAMP => LONG_TYPE_INFO
      case _ => INT_TYPE_INFO
    }
    val returnMethod = getLogicalReturnType(call) match {
      case TIMESTAMP => timestampMethod
      case _ => dateMethod
    }
    val unit = getSymbolOperand(call, 1).asInstanceOf[TimeUnitRange]
    generateCallIfArgsNotNull(codeGenerator.nullCheck, returnType, operands) {
      (operandResultTerms) =>
      unit match {
        case TimeUnitRange.YEAR | TimeUnitRange.MONTH =>
          ???
        case _ =>
          s"""
            |${method.getDeclaringClass.getCanonicalName}.
            |  ${method.getName}(${operandResultTerms.head}, ${unit.startUnit.multiplier})
            |""".stripMargin
      }
    }
  }

}
