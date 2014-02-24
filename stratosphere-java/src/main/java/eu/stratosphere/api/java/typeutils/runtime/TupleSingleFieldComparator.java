/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;


public final class TupleSingleFieldComparator<T extends Tuple, K> extends TypeComparator<T>
	implements java.io.Serializable
{

	private static final long serialVersionUID = 1L;


	private final int keyPosition;
	
	private final TypeComparator<K> comparator;
	
	
		
	public TupleSingleFieldComparator(int keyPosition, TypeComparator<K> comparator) {
		this.keyPosition = keyPosition;
		this.comparator = comparator;
	}

	public int getKeyPosition() {
		return this.keyPosition;
	}
	
	public TypeComparator<K> getComparator() {
		return this.comparator;
	}
	
	@Override
	public int hash(T value) {
		return comparator.hash(value.<K>getField(keyPosition));
		
	}

	@Override
	public void setReference(T toCompare) {
		this.comparator.setReference(toCompare.<K>getField(keyPosition));
	}

	@Override
	public boolean equalToReference(T candidate) {
		return this.comparator.equalToReference(candidate.<K>getField(keyPosition));
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		return this.comparator.compareToReference(((TupleSingleFieldComparator<T, K>) referencedComparator).comparator);
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return this.comparator.compare(firstSource, secondSource);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return this.comparator.supportsNormalizedKey();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return this.comparator.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return this.comparator.isNormalizedKeyPrefixOnly(keyBytes);
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		this.comparator.putNormalizedKey(record.<K>getField(keyPosition), target, offset, numBytes);
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}


	@Override
	public boolean invertNormalizedKey() {
		return this.comparator.invertNormalizedKey();
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new TupleSingleFieldComparator<T, K>(keyPosition, comparator);
	}
}
