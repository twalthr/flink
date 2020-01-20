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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * Utilities for quick access of commonly used instances (like {@link FlinkTypeFactory}) without
 * long chains of getters like {@code (FlinkTypeFactory) agg.getCluster.getTypeFactory()}.
 */
public final class ShortcutUtils {

	public static FlinkTypeFactory getTypeFactory(RelNode relNode) {
		return getTypeFactory(relNode.getCluster());
	}

	public static FlinkTypeFactory getTypeFactory(RelOptCluster cluster) {
		return getTypeFactory(cluster.getTypeFactory());
	}

	public static FlinkTypeFactory getTypeFactory(RelDataTypeFactory typeFactory) {
		return (FlinkTypeFactory) typeFactory;
	}

	private ShortcutUtils() {
		// no instantiation
	}
}
