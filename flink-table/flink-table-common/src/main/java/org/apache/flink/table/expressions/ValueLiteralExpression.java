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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.typeutils.RowIntervalTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.table.utils.TypeStringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * Expression for constant literal values.
 */
@PublicEvolving
public final class ValueLiteralExpression implements CommonExpression {

	private final Object value;

	private final TypeInformation<?> type;

	public ValueLiteralExpression(Object value) {
		this.value = value;
		this.type = deriveTypeFromValue(value);
	}

	public ValueLiteralExpression(Object value, TypeInformation<?> type) {
		this.value = value;
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public TypeInformation<?> getType() {
		return type;
	}

	@Override
	public List<CommonExpression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitValueLiteral(this);
	}

	@Override
	public String toString() {
		if (value == null) {
			return "null";
		}
		if (type == SqlTimeTypeInfo.DATE) {
			return stringifyValue(value.toString()) + ".toDate";
		} else if (type == SqlTimeTypeInfo.TIME) {
			return stringifyValue(value.toString()) + ".toTime";
		} else if (type == SqlTimeTypeInfo.TIMESTAMP) {
			return stringifyValue(value.toString()) + ".toTimestamp";
		} else if (type == TimeIntervalTypeInfo.INTERVAL_MILLIS) {
			return value + ".millis";
		} else if (type == TimeIntervalTypeInfo.INTERVAL_MONTHS) {
			return value + ".months";
		} else if (type == RowIntervalTypeInfo.INTERVAL_ROWS) {
			return value + ".rows";
		} else {
			return ValueLiteralExpression.class.getSimpleName() + "(" +
				stringifyValue(value) + ", " + TypeStringUtils.writeTypeInfo(type) + ")";
		}
	}

	private static String stringifyValue(Object value) {
		if (value instanceof String) {
			return "'" + value + "'";
		}
		return value.toString();
	}

	private static TypeInformation<?> deriveTypeFromValue(Object value) {
		if (value == null) {
			throw new IllegalArgumentException(
				"Cannot derive a type from a null value. The type must be specified explicitly.");
		} else if (value instanceof String) {
			return Types.STRING;
		} else if (value instanceof Long) {
			return Types.LONG;
		} else if (value instanceof Integer) {
			return Types.INT;
		} else if (value instanceof Short) {
			return Types.SHORT;
		} else if (value instanceof Byte) {
			return Types.BYTE;
		} else if (value instanceof Double) {
			return Types.DOUBLE;
		} else if (value instanceof Float) {
			return Types.FLOAT;
		} else if (value instanceof Boolean) {
			return Types.BOOLEAN;
		} else if (value instanceof BigDecimal) {
			return Types.BIG_DEC;
		} else if (value instanceof Date) {
			return Types.SQL_DATE;
		} else if (value instanceof Time) {
			return Types.SQL_TIME;
		} else if (value instanceof Timestamp) {
			return Types.SQL_TIMESTAMP;
		} else {
			throw new IllegalArgumentException(
				"Cannot derive a type for value '" + value + "'. The type must be specified explicitly.");
		}
	}
}
