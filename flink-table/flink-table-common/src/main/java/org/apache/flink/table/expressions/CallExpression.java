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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * General expression for calling a function. A call expression contains all information required to
 * validate a function call and perform planning. The function can be a built-in function or a user-defined
 * function.
 *
 * <p>Equals/hashCode support of this expression depends on the equals/hashCode support of the function
 * definition.
 *
 * <p>A call expression can store information about the origin of a function definition using an
 * {@link ObjectIdentifier} to make the call expression persistable.
 */
@PublicEvolving
public final class CallExpression implements ResolvedExpression {

	private final @Nullable ObjectIdentifier objectIdentifier;

	private final FunctionDefinition functionDefinition;

	private final List<ResolvedExpression> args;

	private final DataType dataType;

	public CallExpression(
			ObjectIdentifier objectIdentifier,
			FunctionDefinition functionDefinition,
			List<ResolvedExpression> args,
			DataType dataType) {
		this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier, "Object identifier must not be null.");
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
		this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
	}

	public CallExpression(
			FunctionDefinition functionDefinition,
			List<ResolvedExpression> args,
			DataType dataType) {
		this.objectIdentifier = null;
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
		this.dataType = Preconditions.checkNotNull(dataType, "Data type must not be null.");
	}

	public CallExpression replaceArgs(List<ResolvedExpression> args) {
		if (objectIdentifier == null) {
			return new CallExpression(functionDefinition, args, dataType);
		}
		return new CallExpression(objectIdentifier, functionDefinition, args, dataType);
	}

	public Optional<ObjectIdentifier> getObjectIdentifier() {
		return Optional.ofNullable(objectIdentifier);
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public String getFunctionSummary() {
		if (objectIdentifier == null) {
			return "*" + functionDefinition.toString() + "*";
		} else {
			return objectIdentifier.asSerializableString();
		}
	}

	@Override
	public DataType getOutputDataType() {
		return dataType;
	}

	@Override
	public String asSummaryString() {
		final List<String> argList = args.stream().map(Object::toString).collect(Collectors.toList());
		return getFunctionSummary() + "(" + String.join(", ", argList) + ")";
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.unmodifiableList(this.args);
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitCall(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CallExpression that = (CallExpression) o;
		return Objects.equals(objectIdentifier, that.objectIdentifier) &&
			functionDefinition.equals(that.functionDefinition) &&
			args.equals(that.args) &&
			dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(objectIdentifier, functionDefinition, args, dataType);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
