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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.LongValue;


public final class LongValueSerializer extends TypeSerializerSingleton<LongValue> {

	private static final long serialVersionUID = 1L;

	public static final LongValueSerializer INSTANCE = new LongValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public boolean canCreateInstance() {
		return true;
	}

	@Override
	public LongValue createInstance() {
		return new LongValue();
	}

	@Override
	public LongValue copy(LongValue from) {
		LongValue result = new LongValue();
		result.setValue(from.getValue());
		return result;
	}

	@Override
	public LongValue copy(LongValue from, LongValue reuse) {
		if (reuse == null) {
			reuse = copy(from);
		} else {
			reuse.setValue(from.getValue());
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(LongValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public LongValue deserialize(DataInputView source) throws IOException {
		LongValue result = new LongValue();
		result.read(source);
		return result;
	}

	@Override
	public LongValue deserialize(LongValue reuse, DataInputView source) throws IOException {
		if (reuse == null) {
			reuse = deserialize(source);
		}
		else {
			reuse.read(source);
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
	}
}
