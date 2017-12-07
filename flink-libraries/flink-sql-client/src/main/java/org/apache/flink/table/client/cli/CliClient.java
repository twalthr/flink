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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

import java.io.IOError;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * SQL CLI client.
 */
public class CliClient {

	private final Executor executor;

	private final SessionContext context;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private boolean isRunning;

	private static final int PLAIN_TERMINAL_WIDTH = 80;

	private static final int PLAIN_TERMINAL_HEIGHT = 30;

	public CliClient(SessionContext context, Executor executor) {
		this.context = context;
		this.executor = executor;

		try {
			// initialize terminal
			terminal = TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}

		// initialize line lineReader
		final DefaultParser parser = new DefaultParser();
		parser.setEofOnEscapedNewLine(true); // allows for multi-line commands
		lineReader = LineReaderBuilder.builder()
			.terminal(terminal)
			.appName(CliStrings.CLI_NAME)
			.parser(parser)
			.build();

		// create prompt
		prompt = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
			.append("Flink SQL")
			.style(AttributedStyle.DEFAULT)
			.append("> ")
			.toAnsi();
	}

	public Terminal getTerminal() {
		return terminal;
	}

	public SessionContext getContext() {
		return context;
	}

	public void clearTerminal() {
		if (isPlainTerminal()) {
			for (int i = 0; i < 200; i++) { // large number of empty lines
				terminal.writer().println();
			}
		} else {
			terminal.puts(InfoCmp.Capability.clear_screen);
		}
	}

	public boolean isPlainTerminal() {
		// check if terminal width can be determined
		// e.g. IntelliJ IDEA terminal supports only a plain terminal
		return terminal.getWidth() == 0 && terminal.getHeight() == 0;
	}

	public int getWidth() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_WIDTH;
		}
		return terminal.getWidth();
	}

	public int getHeight() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_HEIGHT;
		}
		return terminal.getHeight();
	}

	public Executor getExecutor() {
		return executor;
	}

	public void open() {
		isRunning = true;

		// print welcome
		terminal.writer().append(CliStrings.MESSAGE_WELCOME);

		// begin reading loop
		while (isRunning) {
			// make some space to previous command
			terminal.writer().append("\n");
			terminal.flush();

			String line;
			try {
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
			} catch (UserInterruptException | EndOfFileException | IOError e) {
				// user cancelled application with Ctrl+C or kill
				break;
			} catch (Throwable t) {
				throw new SqlClientException("Could not read from command line.", t);
			}
			if (line == null || line.equals("")) {
				continue;
			}

			final SqlCommandCall cmdCall = SqlCommandParser.parse(line);

			if (cmdCall == null) {
				terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_UNKNOWN_SQL));
				continue;
			}

			switch (cmdCall.command) {
				case QUIT:
					callQuit(cmdCall);
					break;
				case CLEAR:
					callClear(cmdCall);
					break;
				case RESET:
					callReset(cmdCall);
					break;
				case SET:
					callSet(cmdCall);
					break;
				case HELP:
					callHelp(cmdCall);
					break;
				case SHOW_TABLES:
					callShowTables(cmdCall);
					break;
				case DESCRIBE:
					callDescribe(cmdCall);
					break;
				case EXPLAIN:
					callExplain(cmdCall);
					break;
				case SELECT:
					callSelect(cmdCall);
					break;
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private void callQuit(SqlCommandCall cmdCall) {
		terminal.writer().println(CliStrings.MESSAGE_QUIT);
		terminal.flush();
		isRunning = false;
	}

	private void callClear(SqlCommandCall cmdCall) {
		clearTerminal();
	}

	private void callReset(SqlCommandCall cmdCall) {
		context.resetSessionProperties();
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_RESET).toAnsi());
	}

	private void callSet(SqlCommandCall cmdCall) {
		// show all properties
		if (cmdCall.operands.length == 0) {
			final Map<String, String> properties;
			try {
				properties = executor.getSessionProperties(context);
			} catch (SqlExecutionException e) {
				printException(e);
				return;
			}
			if (properties.isEmpty()) {
				terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
			} else {
				properties
					.entrySet()
					.stream()
					.map((e) -> e.getKey() + "=" + e.getValue())
					.sorted()
					.forEach((p) -> terminal.writer().println(p));
			}
		}
		// set a property
		else {
			context.setSessionProperty(cmdCall.operands[0], cmdCall.operands[1]);
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_SET).toAnsi());
		}
		terminal.flush();
	}

	private void callHelp(SqlCommandCall cmdCall) {
		terminal.writer().println(CliStrings.MESSAGE_HELP);
		terminal.flush();
	}

	private void callShowTables(SqlCommandCall cmdCall) {
		final List<String> tables;
		try {
			tables = executor.listTables(context);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		if (tables.isEmpty()) {
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		} else {
			tables.forEach((v) -> terminal.writer().println(v));
		}
		terminal.flush();
	}

	private void callDescribe(SqlCommandCall cmdCall) {
		final TableSchema schema;
		try {
			schema = executor.getTableSchema(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		terminal.writer().println(schema.toString());
		terminal.flush();
	}

	private void callExplain(SqlCommandCall cmdCall) {
		final String explanation;
		try {
			explanation = executor.explainStatement(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		terminal.writer().println(explanation);
		terminal.flush();
	}

	private void callSelect(SqlCommandCall cmdCall) {
		final ResultDescriptor resultDesc;
		try {
			resultDesc = executor.executeQuery(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}
		final CliResultView view;
		if (resultDesc.isChangelog()) {
			view = new CliChangelogResultView(this, resultDesc);
		} else {
			view = new CliTableResultView(this, resultDesc);
		}

		// enter view
		try {
			view.open();
		} catch (SqlExecutionException e) {
			printException(e);
			return;
		}

		// view left
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_RESULT_QUIT).toAnsi());
		terminal.flush();
	}

	// --------------------------------------------------------------------------------------------

	private void printException(SqlExecutionException e) {
		terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e).toAnsi());
		terminal.flush();
	}
}
