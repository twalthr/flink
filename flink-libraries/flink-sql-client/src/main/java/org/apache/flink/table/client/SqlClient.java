package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.LocalExecutor;
import org.apache.flink.table.client.gateway.SessionContext;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * <p>- In gateway mode, the SQL CLI client connects to the REST API of the gateway and allows for
 * managing queries via console.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional components.
 */
public class SqlClient {

	private final boolean isEmbedded;
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";

	public static final String DEFAULT_SESSION_ID = "default";

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
	}

	private void start() {
		if (isEmbedded) {
			// create local executor with default environment
			final List<URL> jars;
			if (options.getJars() != null) {
				jars = options.getJars();
			} else {
				jars = Collections.emptyList();
			}
			final List<URL> libDirs;
			if (options.getLibraryDirs() != null) {
				libDirs = options.getLibraryDirs();
			} else {
				libDirs = Collections.emptyList();
			}
			final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
			executor.start();

			// create CLI client with session environment
			final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
			final SessionContext context;
			if (options.getSessionId() == null) {
				context = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
			} else {
				context = new SessionContext(options.getSessionId(), sessionEnv);
			}

			// add shutdown hook
			Runtime.getRuntime().addShutdownHook(new EmbeddedShutdownThread(context, executor));

			// start CLI
			final CliClient cli = new CliClient(context, executor);
			cli.open();
		} else {
			throw new SqlClientException("Gateway mode is not supported yet.");
		}
	}

	// --------------------------------------------------------------------------------------------

	private static void shutdown(SessionContext context, Executor executor) {
		System.out.println();
		System.out.print("Shutting down executor...");
		executor.stop(context);
		System.out.println("done.");
	}

	private static Environment readSessionEnvironment(URL envUrl) {
		// use an empty environment by default
		if (envUrl == null) {
			System.out.println("No session environment specified.");
			return new Environment();
		}

		System.out.println("Reading session environment from: " + envUrl);
		try {
			return Environment.parse(envUrl);
		} catch (IOException e) {
			throw new SqlClientException("Could not read session environment file at: " + envUrl, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
			return;
		}

		switch (args[0]) {

			case MODE_EMBEDDED:
				// remove mode
				final String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
				} else {
					try {
						final SqlClient client = new SqlClient(true, options);
						client.start();
					} catch (SqlClientException e) {
						// make space in terminal
						System.out.println();
						System.out.println();
						throw e;
					} catch (Throwable t) {
						// make space in terminal
						System.out.println();
						System.out.println();
						throw new SqlClientException("Unexpected exception. This is a bug. Please consider filing an issue.", t);
					}
				}
				break;

			case MODE_GATEWAY:
				throw new SqlClientException("Gateway mode is not supported yet.");

			default:
				CliOptionsParser.printHelpClient();
		}
	}

	// --------------------------------------------------------------------------------------------

	private class EmbeddedShutdownThread extends Thread {

		private final SessionContext context;
		private final Executor executor;

		public EmbeddedShutdownThread(SessionContext context, Executor executor) {
			this.context = context;
			this.executor = executor;
		}

		@Override
		public void run() {
			shutdown(context, executor);
		}
	}
}
