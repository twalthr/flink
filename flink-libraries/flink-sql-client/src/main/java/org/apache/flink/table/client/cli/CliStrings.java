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

import org.apache.flink.util.ExceptionUtils;

import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Utility class that contains all strings for CLI commands and messages.
 */
public final class CliStrings {

	private CliStrings() {
		// private
	}

	public static final String CLI_NAME = "Flink SQL CLI Client";
	public static final String DEFAULT_MARGIN = " ";

	// --------------------------------------------------------------------------------------------

	public static final String COMMAND_QUIT = "QUIT";

	public static final String COMMAND_CLEAR = "CLEAR";

	public static final String COMMAND_HELP = "HELP";

	public static final String COMMAND_SHOW_TABLES = "SHOW TABLES";

	public static final String COMMAND_DESCRIBE = "DESCRIBE";

	public static final String COMMAND_EXPLAIN = "EXPLAIN";

	public static final String COMMAND_SELECT = "SELECT";

	// --------------------------------------------------------------------------------------------

	public static final String MESSAGE_HELP = new AttributedStringBuilder()
		.append("The following commands are available:\n\n")
		.ansiAppend(formatCommand(COMMAND_QUIT, "Quits the SQL CLI client."))
		.ansiAppend(formatCommand(COMMAND_CLEAR, "Clears the current terminal."))
		.ansiAppend(formatCommand(COMMAND_HELP, "Prints the available commands."))
		.ansiAppend(formatCommand(COMMAND_SHOW_TABLES, "Shows all registered tables."))
		.ansiAppend(formatCommand(COMMAND_DESCRIBE, "Describes the schema of a table with the given name."))
		.ansiAppend(formatCommand(COMMAND_EXPLAIN, "Describes the execution plan of a query or table with the given name."))
		.ansiAppend(formatCommand(COMMAND_SELECT, "Executes a SQL SELECT query on the Flink cluster."))
		.style(AttributedStyle.DEFAULT.underline())
		.ansiAppend("\nHint")
		.style(AttributedStyle.DEFAULT)
		.append(": Use '\\' for multi-line commands.")
		.toAnsi();

	public static final String MESSAGE_WELCOME = "                                   \u2592\u2593\u2588\u2588\u2593\u2588\u2588\u2592\n" +
		"                               \u2593\u2588\u2588\u2588\u2588\u2592\u2592\u2588\u2593\u2592\u2593\u2588\u2588\u2588\u2593\u2592\n" +
		"                            \u2593\u2588\u2588\u2588\u2593\u2591\u2591        \u2592\u2592\u2592\u2593\u2588\u2588\u2592  \u2592\n" +
		"                          \u2591\u2588\u2588\u2592   \u2592\u2592\u2593\u2593\u2588\u2593\u2593\u2592\u2591      \u2592\u2588\u2588\u2588\u2588\n" +
		"                          \u2588\u2588\u2592         \u2591\u2592\u2593\u2588\u2588\u2588\u2592    \u2592\u2588\u2592\u2588\u2592\n" +
		"                            \u2591\u2593\u2588            \u2588\u2588\u2588   \u2593\u2591\u2592\u2588\u2588\n" +
		"                              \u2593\u2588       \u2592\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591\u2592\u2591\u2593\u2593\u2588\n" +
		"                            \u2588\u2591 \u2588   \u2592\u2592\u2591       \u2588\u2588\u2588\u2593\u2593\u2588 \u2592\u2588\u2592\u2592\u2592\n" +
		"                            \u2588\u2588\u2588\u2588\u2591   \u2592\u2593\u2588\u2593      \u2588\u2588\u2592\u2592\u2592 \u2593\u2588\u2588\u2588\u2592\n" +
		"                         \u2591\u2592\u2588\u2593\u2593\u2588\u2588       \u2593\u2588\u2592    \u2593\u2588\u2592\u2593\u2588\u2588\u2593 \u2591\u2588\u2591\n" +
		"                   \u2593\u2591\u2592\u2593\u2588\u2588\u2588\u2588\u2592 \u2588\u2588         \u2592\u2588    \u2588\u2593\u2591\u2592\u2588\u2592\u2591\u2592\u2588\u2592\n" +
		"                  \u2588\u2588\u2588\u2593\u2591\u2588\u2588\u2593  \u2593\u2588           \u2588   \u2588\u2593 \u2592\u2593\u2588\u2593\u2593\u2588\u2592\n" +
		"                \u2591\u2588\u2588\u2593  \u2591\u2588\u2591            \u2588  \u2588\u2592 \u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2592 \u2588\u2588\u2593\u2591\u2592\n" +
		"               \u2588\u2588\u2588\u2591 \u2591 \u2588\u2591          \u2593 \u2591\u2588 \u2588\u2588\u2588\u2588\u2588\u2592\u2591\u2591    \u2591\u2588\u2591\u2593  \u2593\u2591\n" +
		"              \u2588\u2588\u2593\u2588 \u2592\u2592\u2593\u2592          \u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2591       \u2592\u2588\u2592 \u2592\u2593 \u2593\u2588\u2588\u2593\n" +
		"           \u2592\u2588\u2588\u2593 \u2593\u2588 \u2588\u2593\u2588       \u2591\u2592\u2588\u2588\u2588\u2588\u2588\u2593\u2593\u2592\u2591         \u2588\u2588\u2592\u2592  \u2588 \u2592  \u2593\u2588\u2592\n" +
		"           \u2593\u2588\u2593  \u2593\u2588 \u2588\u2588\u2593 \u2591\u2593\u2593\u2593\u2593\u2593\u2593\u2593\u2592              \u2592\u2588\u2588\u2593           \u2591\u2588\u2592\n" +
		"           \u2593\u2588    \u2588 \u2593\u2588\u2588\u2588\u2593\u2592\u2591              \u2591\u2593\u2593\u2593\u2588\u2588\u2588\u2593          \u2591\u2592\u2591 \u2593\u2588\n" +
		"           \u2588\u2588\u2593    \u2588\u2588\u2592    \u2591\u2592\u2593\u2593\u2588\u2588\u2588\u2593\u2593\u2593\u2593\u2593\u2588\u2588\u2588\u2588\u2588\u2588\u2593\u2592            \u2593\u2588\u2588\u2588  \u2588\n" +
		"          \u2593\u2588\u2588\u2588\u2592 \u2588\u2588\u2588   \u2591\u2593\u2593\u2592\u2591\u2591   \u2591\u2593\u2588\u2588\u2588\u2588\u2593\u2591                  \u2591\u2592\u2593\u2592  \u2588\u2593\n" +
		"          \u2588\u2593\u2592\u2592\u2593\u2593\u2588\u2588  \u2591\u2592\u2592\u2591\u2591\u2591\u2592\u2592\u2592\u2592\u2593\u2588\u2588\u2593\u2591                            \u2588\u2593\n" +
		"          \u2588\u2588 \u2593\u2591\u2592\u2588   \u2593\u2593\u2593\u2593\u2592\u2591\u2591  \u2592\u2588\u2593       \u2592\u2593\u2593\u2588\u2588\u2593    \u2593\u2592          \u2592\u2592\u2593\n" +
		"          \u2593\u2588\u2593 \u2593\u2592\u2588  \u2588\u2593\u2591  \u2591\u2592\u2593\u2593\u2588\u2588\u2592            \u2591\u2593\u2588\u2592   \u2592\u2592\u2592\u2591\u2592\u2592\u2593\u2588\u2588\u2588\u2588\u2588\u2592\n" +
		"           \u2588\u2588\u2591 \u2593\u2588\u2592\u2588\u2592  \u2592\u2593\u2593\u2592  \u2593\u2588                \u2588\u2591      \u2591\u2591\u2591\u2591   \u2591\u2588\u2592\n" +
		"           \u2593\u2588   \u2592\u2588\u2593   \u2591     \u2588\u2591                \u2592\u2588              \u2588\u2593\n" +
		"            \u2588\u2593   \u2588\u2588         \u2588\u2591                 \u2593\u2593        \u2592\u2588\u2593\u2593\u2593\u2592\u2588\u2591\n" +
		"             \u2588\u2593 \u2591\u2593\u2588\u2588\u2591       \u2593\u2592                  \u2593\u2588\u2593\u2592\u2591\u2591\u2591\u2592\u2593\u2588\u2591    \u2592\u2588\n" +
		"              \u2588\u2588   \u2593\u2588\u2593\u2591      \u2592                    \u2591\u2592\u2588\u2592\u2588\u2588\u2592      \u2593\u2593\n" +
		"               \u2593\u2588\u2592   \u2592\u2588\u2593\u2592\u2591                         \u2592\u2592 \u2588\u2592\u2588\u2593\u2592\u2592\u2591\u2591\u2592\u2588\u2588\n" +
		"                \u2591\u2588\u2588\u2592    \u2592\u2593\u2593\u2592                     \u2593\u2588\u2588\u2593\u2592\u2588\u2592 \u2591\u2593\u2593\u2593\u2593\u2592\u2588\u2593\n" +
		"                  \u2591\u2593\u2588\u2588\u2592                          \u2593\u2591  \u2592\u2588\u2593\u2588  \u2591\u2591\u2592\u2592\u2592\n" +
		"                      \u2592\u2593\u2593\u2593\u2593\u2593\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2592\u2591\u2591\u2593\u2593  \u2593\u2591\u2592\u2588\u2591\n" +
		"          \n" +
		"    ______ _ _       _       _____  ____  _         _____ _ _            _   \n" +
		"   |  ____| (_)     | |     / ____|/ __ \\| |       / ____| (_)          | |  \n" +
		"   | |__  | |_ _ __ | | __ | (___ | |  | | |      | |    | |_  ___ _ __ | |_ \n" +
		"   |  __| | | | '_ \\| |/ /  \\___ \\| |  | | |      | |    | | |/ _ \\ '_ \\| __|\n" +
		"   | |    | | | | | |   <   ____) | |__| | |____  | |____| | |  __/ | | | |_ \n" +
		"   |_|    |_|_|_| |_|_|\\_\\ |_____/ \\___\\_\\______|  \\_____|_|_|\\___|_| |_|\\__|\n" +
		"          \n" +
		"        Welcome! Enter HELP to list all available commands. QUIT to exit.\n\n";

	public static final String MESSAGE_QUIT = "Exiting " + CliStrings.CLI_NAME + "...";

	public static final String MESSAGE_SQL_EXECUTION_ERROR = "Could not execute SQL statement.";

	public static final String MESSAGE_EMPTY = "Result was empty.";

	public static final String MESSAGE_UNKNOWN_SQL = "Unknown SQL statement.";

	public static final String MESSAGE_UNKNOWN_TABLE = "Unknown table.";

	public static final String MESSAGE_RESULT_SNAPSHOT_ERROR = "Could not create a snapshot of the dynamic table.";

	public static final String MESSAGE_RESULT_QUIT = "Result retrieval and table program cancelled.";

	public static final String MESSAGE_RESULT_TIMEOUT = "Result retrieval reached timeout.";

	public static String messageInfo(String message) {
		return new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.BLUE))
			.append("[INFO] ")
			.append(message)
			.toAnsi();
	}

	public static String messageError(String message, Throwable t) {
		return messageError(message, ExceptionUtils.stringifyException(t));
	}

	public static String messageError(String message) {
		return messageError(message, (String) null);
	}

	public static String messageError(String message, String s) {
		final AttributedStringBuilder builder = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.bold().foreground(AttributedStyle.RED))
			.append("[ERROR] ")
			.append(message);

		if (s != null) {
			builder
				.append(" Reason:\n")
				.append(s);
		}

		return builder.toAnsi();
	}

	// --------------------------------------------------------------------------------------------

	public static final String RESULT_TITLE = "SQL Query Result";

	public static final String RESULT_REFRESH_INTERVAL = "Refresh:";

	public static final String RESULT_PAGE = "Page:";

	public static final String RESULT_PAGE_OF = " of ";

	public static final String RESULT_LAST_REFRESH = "Updated:";

	public static final String RESULT_LAST_PAGE = "Last";

	public static final String RESULT_QUIT = "Quit";

	public static final String RESULT_REFRESH = "Refresh";

	public static final String RESULT_GOTO = "Goto Page";

	public static final String RESULT_NEXT = "Next Page";

	public static final String RESULT_PREV = "Prev Page";

	public static final String RESULT_LAST = "Last Page";

	public static final String RESULT_FIRST = "First Page";

	public static final String RESULT_SEARCH = "Search";

	public static final String RESULT_INC_REFRESH = "Inc Refresh"; // implementation assumes max length of 11

	public static final String RESULT_DEC_REFRESH = "Dec Refresh";

	public static final String RESULT_OPEN = "Open Row";

	// --------------------------------------------------------------------------------------------

	private static String formatCommand(String command, String description) {
		return new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.bold())
			.append(command)
			.append("\t\t")
			.style(AttributedStyle.DEFAULT)
			.append(description)
			.append('\n')
			.toAnsi();
	}
}
