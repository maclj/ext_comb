package com.hadoop.plat.util.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * A simple shell command executor.
 * 
 * <code>ShellCommandExecutor</code>should be used in cases where the output of
 * the command needs no explicit parsing and where the command, working
 * directory and the environment remains unchanged. The output of the command is
 * stored as-is and is expected to be small.
 */
public class ShellCommandExecutor extends Shell implements CommandExecutor {

	private String[] command;
	private StringBuffer output;

	public ShellCommandExecutor(String[] execString) {
		this(execString, null);
	}

	public ShellCommandExecutor(String[] execString, File dir) {
		this(execString, dir, null);
	}

	public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env) {
		this(execString, dir, env, 0L);
	}

	/**
	 * Create a new instance of the ShellCommandExecutor to execute a command.
	 * 
	 * @param execString
	 *            The command to execute with arguments
	 * @param dir
	 *            If not-null, specifies the directory which should be set as
	 *            the current working directory for the command. If null, the
	 *            current working directory is not modified.
	 * @param env
	 *            If not-null, environment of the command will include the
	 *            key-value pairs specified in the map. If null, the current
	 *            environment is not modified.
	 * @param timeout
	 *            Specifies the time in milliseconds, after which the command
	 *            will be killed and the status marked as timedout. If 0, the
	 *            command will not be timed out.
	 */
	public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env, long timeout) {
		command = execString.clone();
		if (dir != null) {
			setWorkingDirectory(dir);
		}
		if (env != null) {
			setEnvironment(env);
		}
		timeOutInterval = timeout;
	}

	/** Execute the shell command. */
	public void execute() throws IOException {
		this.run();
	}

	@Override
	public String[] getExecString() {
		return command;
	}

	@Override
	protected void parseExecResult(BufferedReader lines) throws IOException {
		output = new StringBuffer();
		char[] buf = new char[512];
		int nRead;
		while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
			output.append(buf, 0, nRead);
		}
	}

	/** Get the output of the shell command. */
	public String getOutput() {
		return (output == null) ? "" : output.toString();
	}

	/**
	 * Returns the commands of this instance. Arguments with spaces in are
	 * presented with quotes round; other arguments are presented raw
	 *
	 * @return a string representation of the object.
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		String[] args = getExecString();
		for (String s : args) {
			if (s.indexOf(' ') >= 0) {
				builder.append('"').append(s).append('"');
			} else {
				builder.append(s);
			}
			builder.append(' ');
		}
		return builder.toString();
	}

	@Override
	public void close() {
	}
}
