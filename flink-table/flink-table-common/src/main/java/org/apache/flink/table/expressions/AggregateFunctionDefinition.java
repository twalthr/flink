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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * The function definition of an user-defined aggregate function.
 */
@PublicEvolving
public final class AggregateFunctionDefinition implements FunctionDefinition {

	private final AggregateFunction<?, ?> aggregateFunction;
	private final TypeInformation<?> resultTypeInfo;
	private final TypeInformation<?> accumulatorTypeInfo;

	public AggregateFunctionDefinition(
			AggregateFunction<?, ?> aggregateFunction,
			TypeInformation<?> resultTypeInfo,
			TypeInformation<?> accTypeInfo) {
		this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
		this.resultTypeInfo = Preconditions.checkNotNull(resultTypeInfo);
		this.accumulatorTypeInfo = Preconditions.checkNotNull(accTypeInfo);
	}

	public AggregateFunction<?, ?> getAggregateFunction() {
		return aggregateFunction;
	}

	public TypeInformation<?> getResultTypeInfo() {
		return resultTypeInfo;
	}

	public TypeInformation<?> getAccumulatorTypeInfo() {
		return accumulatorTypeInfo;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.AGGREGATE_FUNCTION;
	}

	@Override
	public String toString() {
		return aggregateFunction.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AggregateFunctionDefinition that = (AggregateFunctionDefinition) o;
		return aggregateFunction.equals(that.aggregateFunction) &&
			resultTypeInfo.equals(that.resultTypeInfo) &&
			accumulatorTypeInfo.equals(that.accumulatorTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(aggregateFunction, resultTypeInfo, accumulatorTypeInfo);
	}
}
