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

package org.apache.flink.api.table.test

import java.util.Date

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.{TrimType, DateTimeUnit, Expression}
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.test.utils.ExpressionEvaluator
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.junit.Assert.assertEquals
import org.junit.Test

class ScalarFunctionsTest {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSubstring(): Unit = {
    testFunction(
      'f0.substring(2),
      "f0.substring(2)",
      "SUBSTRING(f0, 2)",
      "his is a test String.")

    testFunction(
      'f0.substring(2, 5),
      "f0.substring(2, 5)",
      "SUBSTRING(f0, 2, 5)",
      "his i")

    testFunction(
      'f0.substring(1, 'f7),
      "f0.substring(1, f7)",
      "SUBSTRING(f0, 1, f7)",
      "Thi")
  }

  @Test
  def testTrim(): Unit = {
    testFunction(
      'f8.trim(),
      "f8.trim()",
      "TRIM(f8)",
      "This is a test String.")

    testFunction(
      'f8.trim(TrimType.BOTH, " "),
      "trim(f8)",
      "TRIM(f8)",
      "This is a test String.")

    testFunction(
      'f8.trim(TrimType.TRAILING, " "),
      "f8.trim(TRAILING, ' ')",
      "TRIM(TRAILING FROM f8)",
      " This is a test String.")

    testFunction(
      'f0.trim(TrimType.BOTH, "."),
      "trim(BOTH, '.', f0)",
      "TRIM(BOTH '.' FROM f0)",
      "This is a test String")
  }

  @Test
  def testCharLength(): Unit = {
    testFunction(
      'f0.charLength(),
      "f0.charLength()",
      "CHAR_LENGTH(f0)",
      "22")

    testFunction(
      'f0.charLength(),
      "charLength(f0)",
      "CHARACTER_LENGTH(f0)",
      "22")
  }

  @Test
  def testUpperCase(): Unit = {
    testFunction(
      'f0.upperCase(),
      "f0.upperCase()",
      "UPPER(f0)",
      "THIS IS A TEST STRING.")
  }

  @Test
  def testLowerCase(): Unit = {
    testFunction(
      'f0.lowerCase(),
      "f0.lowerCase()",
      "LOWER(f0)",
      "this is a test string.")
  }

  @Test
  def testInitCap(): Unit = {
    testFunction(
      'f0.initCap(),
      "f0.initCap()",
      "INITCAP(f0)",
      "This Is A Test String.")
  }

  @Test
  def testConcat(): Unit = {
    testFunction(
      'f0 + 'f0,
      "f0 + f0",
      "f0||f0",
      "This is a test String.This is a test String.")
  }

  @Test
  def testLike(): Unit = {
    testFunction(
      'f0.like("Th_s%"),
      "f0.like('Th_s%')",
      "f0 LIKE 'Th_s%'",
      "true")

    testFunction(
      'f0.like("%is a%"),
      "f0.like('%is a%')",
      "f0 LIKE '%is a%'",
      "true")
  }

  @Test
  def testNotLike(): Unit = {
    testFunction(
      !'f0.like("Th_s%"),
      "!f0.like('Th_s%')",
      "f0 NOT LIKE 'Th_s%'",
      "false")

    testFunction(
      !'f0.like("%is a%"),
      "!f0.like('%is a%')",
      "f0 NOT LIKE '%is a%'",
      "false")
  }

  @Test
  def testSimilar(): Unit = {
    testFunction(
      'f0.similar("_*"),
      "f0.similar('_*')",
      "f0 SIMILAR TO '_*'",
      "true")

    testFunction(
      'f0.similar("This (is)? a (test)+ Strin_*"),
      "f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "true")
  }

  @Test
  def testNotSimilar(): Unit = {
    testFunction(
      !'f0.similar("_*"),
      "!f0.similar('_*')",
      "f0 NOT SIMILAR TO '_*'",
      "false")

    testFunction(
      !'f0.similar("This (is)? a (test)+ Strin_*"),
      "!f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "false")
  }

  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testMod(): Unit = {
    testFunction(
      'f4.mod('f7),
      "f4.mod(f7)",
      "MOD(f4, f7)",
      "2")

    testFunction(
      'f4.mod(3),
      "mod(f4, 3)",
      "MOD(f4, 3)",
      "2")

    testFunction(
      'f4 % 3,
      "mod(44, 3)",
      "MOD(44, 3)",
      "2")

  }

  @Test
  def testExp(): Unit = {
    testFunction(
      'f2.exp(),
      "f2.exp()",
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testFunction(
      'f3.exp(),
      "f3.exp()",
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testFunction(
      'f4.exp(),
      "f4.exp()",
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testFunction(
      'f5.exp(),
      "f5.exp()",
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testFunction(
      'f6.exp(),
      "f6.exp()",
      "EXP(f6)",
      math.exp(4.6).toString)

    testFunction(
      'f7.exp(),
      "exp(3)",
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testFunction(
      'f2.log10(),
      "f2.log10()",
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testFunction(
      'f3.log10(),
      "f3.log10()",
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testFunction(
      'f4.log10(),
      "f4.log10()",
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testFunction(
      'f5.log10(),
      "f5.log10()",
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testFunction(
      'f6.log10(),
      "f6.log10()",
      "LOG10(f6)",
      math.log10(4.6).toString)
  }

  @Test
  def testPower(): Unit = {
    testFunction(
      'f2.power('f7),
      "f2.power(f7)",
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testFunction(
      'f3.power('f6),
      "f3.power(f6)",
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testFunction(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)
  }

  @Test
  def testLn(): Unit = {
    testFunction(
      'f2.ln(),
      "f2.ln()",
      "LN(f2)",
      math.log(42.toByte).toString)

    testFunction(
      'f3.ln(),
      "f3.ln()",
      "LN(f3)",
      math.log(43.toShort).toString)

    testFunction(
      'f4.ln(),
      "f4.ln()",
      "LN(f4)",
      math.log(44.toLong).toString)

    testFunction(
      'f5.ln(),
      "f5.ln()",
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testFunction(
      'f6.ln(),
      "f6.ln()",
      "LN(f6)",
      math.log(4.6).toString)
  }

  @Test
  def testAbs(): Unit = {
    testFunction(
      'f2.abs(),
      "f2.abs()",
      "ABS(f2)",
      "42")

    testFunction(
      'f3.abs(),
      "f3.abs()",
      "ABS(f3)",
      "43")

    testFunction(
      'f4.abs(),
      "f4.abs()",
      "ABS(f4)",
      "44")

    testFunction(
      'f5.abs(),
      "f5.abs()",
      "ABS(f5)",
      "4.5")

    testFunction(
      'f6.abs(),
      "f6.abs()",
      "ABS(f6)",
      "4.6")

    testFunction(
      'f9.abs(),
      "f9.abs()",
      "ABS(f9)",
      "42")

    testFunction(
      'f10.abs(),
      "f10.abs()",
      "ABS(f10)",
      "43")

    testFunction(
      'f11.abs(),
      "f11.abs()",
      "ABS(f11)",
      "44")

    testFunction(
      'f12.abs(),
      "f12.abs()",
      "ABS(f12)",
      "4.5")

    testFunction(
      'f13.abs(),
      "f13.abs()",
      "ABS(f13)",
      "4.6")
  }

  @Test
  def testArithmeticFloorCeil(): Unit = {
    testFunction(
      'f5.floor(),
      "f5.floor()",
      "FLOOR(f5)",
      "4.0")

    testFunction(
      'f5.ceil(),
      "f5.ceil()",
      "CEIL(f5)",
      "5.0")

    testFunction(
      'f3.floor(),
      "f3.floor()",
      "FLOOR(f3)",
      "43")

    testFunction(
      'f3.ceil(),
      "f3.ceil()",
      "CEIL(f3)",
      "43")
  }

  // ----------------------------------------------------------------------------------------------
  // Date/Time functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testDateTimeExtract(): Unit = {
    testFunction(
      'f15.extract(DateTimeUnit.YEAR),
      "extract(YEAR, f15)",
      "EXTRACT(YEAR FROM f15)",
      "2003")

    testFunction(
      'f15.extract(DateTimeUnit.MONTH),
      "extract(MONTH, f15)",
      "EXTRACT(MONTH FROM f15)",
      "7")

    testFunction(
      'f15.extract(DateTimeUnit.DAY),
      "extract(DAY, f15)",
      "EXTRACT(DAY FROM f15)",
      "23")

    testFunction(
      'f15.extract(DateTimeUnit.HOUR),
      "extract(HOUR, f15)",
      "EXTRACT(HOUR FROM f15)",
      "15")

    testFunction(
      'f15.extract(DateTimeUnit.MINUTE),
      "extract(MINUTE, f15)",
      "EXTRACT(MINUTE FROM f15)",
      "11")

    testFunction(
      'f15.extract(DateTimeUnit.SECOND),
      "extract(SECOND, f15)",
      "EXTRACT(SECOND FROM f15)",
      "53")

    testFunction(
      'f15.extract(DateTimeUnit.SECOND),
      "f15.extract(SECOND)",
      "EXTRACT(SECOND FROM f15)",
      "53")

    testFunction(
      "1990-10-14".cast(DATE_TYPE_INFO).extract(DateTimeUnit.DAY),
      "extract(DAY, '1990-10-14'.cast(DATE))",
      "EXTRACT(DAY FROM DATE '1990-10-14')",
      "14")
  }

  @Test
  def testDateTimeFloorCeil(): Unit = {
    testFunction(
      "46800000",
      "46800000",
      "CEIL(TIME '12:59:56' TO MINUTE)",
      "46800000")

    testFunction(
      "42",
      "42",
      "FLOOR(INTERVAL '3:4:5' HOUR TO SECOND)",
      "42")
  }

  @Test
  def testInterval(): Unit = {
    testFunction(
      "16114", //"2014-02-11".cast(DATE_TYPE_INFO).addInterval("2", DateTimeUnit.DAY), TODO
      "'16114'", //'2014-02-11'.cast(DATE).addInterval('2', DAY)",
      "DATE '2014-02-11' + INTERVAL '2' DAY",
      "16114")

    testFunction(
      "16112", //"2014-02-11".cast(DATE_TYPE_INFO).addInterval("2", DateTimeUnit.DAY), TODO
      "'16112'", //'2014-02-11'.cast(DATE).addInterval('2', DAY)",
      "DATE '2014-02-11' + INTERVAL '60' DAY",
      "16112")
  }

  // ----------------------------------------------------------------------------------------------
  // Other functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testCase(): Unit = {
    testFunction(
      ('f7 === 1).eval(11, ('f7 === 2).eval(4, null)),
      "(f7 === 1).eval(11, (f7 === 2).eval(4, null))",
      "CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END",
      "1")

    /*

    testFunction(
      "1",
      "1",
      "CASE WHEN 'a'='a' THEN 1 END",
      "1")

    testFunction(
      "b",
      "b",
      "CASE 2 WHEN 1 THEN 'a' ELSE 'b' END",
      "b")

    testFunction(
      "42",
      "42",
      "CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END",
      "bcd")

    testFunction(
      "42",
      "42",
      "CASE 1 WHEN 1 THEN 11.2 WHEN 2 THEN 4.543 ELSE NULL END",
      "42")

    testFunction(
      "42",
      "42",
      "CASE 1" +
        "WHEN 1, 2 THEN '1 or 2'" +
        "WHEN 2 THEN 'not possible'" +
        "WHEN 3, 2 THEN '3'" +
        "ELSE 'none of the above'" +
        "END",
      "42")*/
  }

  // ----------------------------------------------------------------------------------------------

  def testFunction(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    val testData = new Row(17)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")
    testData.setField(9, -42.toByte)
    testData.setField(10, -43.toShort)
    testData.setField(11, -44.toLong)
    testData.setField(12, -4.5.toFloat)
    testData.setField(13, -4.6)
    testData.setField(14, -3)
    testData.setField(15, new Date(1058973113000L)) // 2003-07-23 15:11:53
    testData.setField(16, "1990-10-14")

    val typeInfo = new RowTypeInfo(Seq(
      STRING_TYPE_INFO,
      BOOLEAN_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      INT_TYPE_INFO,
      DATE_TYPE_INFO,
      STRING_TYPE_INFO)).asInstanceOf[TypeInformation[Any]]

    val exprResult = ExpressionEvaluator.evaluate(testData, typeInfo, expr)
    assertEquals(expected, exprResult)

    val exprStringResult = ExpressionEvaluator.evaluate(
      testData,
      typeInfo,
      ExpressionParser.parseExpression(exprString))
    assertEquals(expected, exprStringResult)

    val exprSqlResult = ExpressionEvaluator.evaluate(testData, typeInfo, sqlExpr)
    assertEquals(expected, exprSqlResult)
  }



}
