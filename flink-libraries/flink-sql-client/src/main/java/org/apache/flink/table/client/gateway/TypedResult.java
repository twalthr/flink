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

package org.apache.flink.table.client.gateway;

public class TypedResult<P> {

	private final Type type;

	private final P payload;

	private TypedResult(Type type, P payload) {
		this.type = type;
		this.payload = payload;
	}

	public Type getType() {
		return type;
	}

	public P getPayload() {
		return payload;
	}

	// --------------------------------------------------------------------------------------------

	public static <T> TypedResult<T> empty() {
		return new TypedResult<>(Type.EMPTY, null);
	}

	public static <T> TypedResult<T> payload(T payload) {
		return new TypedResult<>(Type.PAYLOAD, payload);
	}

	public static <T> TypedResult<T> endOfStream() {
		return new TypedResult<>(Type.EOS, null);
	}

	// --------------------------------------------------------------------------------------------

	public enum Type {
		PAYLOAD,
		EMPTY,
		EOS
	}
}
