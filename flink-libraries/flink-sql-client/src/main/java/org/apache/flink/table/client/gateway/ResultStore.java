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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.sinks.TableSink;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class ResultStore {

	private Environment currentEnv;
	private Configuration flinkConfig;
	private CollectTableSink collectTableSink;

	public ResultStore(Configuration flinkConfig) {
		this.flinkConfig = flinkConfig;
	}

	public void init(Environment env) {
		if (currentEnv != null) {
			throw new IllegalStateException("Result store already initialized.");
		}
		currentEnv = env;
	}

	public TableSink<?> getTableSink() {
		if (!currentEnv.getExecution().isStreamingExecution()) {
			throw new SqlExecutionException("Emission is only supported in streaming environments yet.");
		}

		// create table sink
		// determine gateway address (and port if possible)
		collectTableSink = new CollectTableSink(getGatewayAddress(), getGatewayPort());

		return collectTableSink;
	}

	private int getGatewayPort() {
		// try to get address from deployment configuration
		return currentEnv.getDeployment().getGatewayPort();
	}

	private InetAddress getGatewayAddress() {
		// try to get address from deployment configuration
		final String address = currentEnv.getDeployment().getGatewayAddress();

		// use manually defined address
		if (!address.isEmpty()) {
			try {
				return InetAddress.getByName(address);
			} catch (UnknownHostException e) {
				throw new SqlClientException("Invalid gateway address '" + address + "' for result retrieval.", e);
			}
		} else {
			// try to get the address by communicating to JobManager
			final String jobManagerAddress= flinkConfig.getString(JobManagerOptions.ADDRESS);
			final int jobManagerPort = flinkConfig.getInteger(JobManagerOptions.PORT);
			if (jobManagerAddress != null && !jobManagerAddress.isEmpty()) {
				try {
					return ConnectionUtils.findConnectingAddress(
						new InetSocketAddress(jobManagerAddress, jobManagerPort),
						currentEnv.getDeployment().getResponseTimeout(),
						400);
				} catch (Exception e) {
					throw new SqlClientException("Could not determine address of the gateway for result retrieval " +
						"by connecting to the job manager. Please specify the gateway address manually.", e);
				}
			} else {
				try {
					return InetAddress.getLocalHost();
				} catch (UnknownHostException e) {
					throw new SqlClientException("Could not determine address of the gateway for result retrieval. " +
						"Please specify the gateway address manually.", e);
				}
			}
		}
	}
}
