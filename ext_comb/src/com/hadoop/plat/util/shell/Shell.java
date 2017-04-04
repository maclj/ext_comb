package com.hadoop.plat.util.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 使用方式：
 * Shell.execCommand(String...) 即可，
 * 
 * 详见各execCommand方法的说明。
 * 
 *
 */
public abstract class Shell {

	public static final Log LOG = LogFactory.getLog(Shell.class);

	private static boolean IS_JAVA7_OR_ABOVE = System.getProperty("java.version").substring(0, 3).compareTo("1.7") >= 0;

	public static boolean isJava7OrAbove() {
		return IS_JAVA7_OR_ABOVE;
	}

	/**
	 * Maximum command line length in Windows KB830473 documents this as 8191
	 */
	public static final int WINDOWS_MAX_SHELL_LENGHT = 8191;

	/**
	 * Checks if a given command (String[]) fits in the Windows maximum command
	 * line length Note that the input is expected to already include space
	 * delimiters, no extra count will be added for delimiters.
	 *
	 * @param commands
	 *            command parts, including any space delimiters
	 */
	public static void checkWindowsCommandLineLength(String... commands) throws IOException {
		int len = 0;
		for (String s : commands) {
			len += s.length();
		}
		if (len > WINDOWS_MAX_SHELL_LENGHT) {
			throw new IOException(String.format(
					"The command line has a length of %d exceeds maximum allowed length of %d. "
							+ "Command starts with: %s",
					len, WINDOWS_MAX_SHELL_LENGHT, join("", commands).substring(0, 100)));
		}
	}

	/**
	 * Concatenates strings, using a separator.
	 *
	 * @param separator
	 *            to join with
	 * @param strings
	 *            to join
	 * @return the joined string
	 */
	static String join(CharSequence separator, String[] strings) {
		// Ideally we don't have to duplicate the code here if array is
		// iterable.
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String s : strings) {
			if (first) {
				first = false;
			} else {
				sb.append(separator);
			}
			sb.append(s);
		}
		return sb.toString();
	}

	/** a Unix command to get the current user's name */
	public final static String USER_NAME_COMMAND = "whoami";

	/** Windows CreateProcess synchronization object */
	public static final Object WindowsProcessLaunchLock = new Object();

	// OSType detection

	public enum OSType {
		OS_TYPE_LINUX, OS_TYPE_WIN, OS_TYPE_SOLARIS, OS_TYPE_MAC, OS_TYPE_FREEBSD, OS_TYPE_OTHER
	}

	public static final OSType osType = getOSType();

	static private OSType getOSType() {
		String osName = System.getProperty("os.name");
		if (osName.startsWith("Windows")) {
			return OSType.OS_TYPE_WIN;
		} else if (osName.contains("SunOS") || osName.contains("Solaris")) {
			return OSType.OS_TYPE_SOLARIS;
		} else if (osName.contains("Mac")) {
			return OSType.OS_TYPE_MAC;
		} else if (osName.contains("FreeBSD")) {
			return OSType.OS_TYPE_FREEBSD;
		} else if (osName.startsWith("Linux")) {
			return OSType.OS_TYPE_LINUX;
		} else {
			// Some other form of Unix
			return OSType.OS_TYPE_OTHER;
		}
	}

	// Helper static vars for each platform
	public static final boolean WINDOWS = (osType == OSType.OS_TYPE_WIN);
	public static final boolean SOLARIS = (osType == OSType.OS_TYPE_SOLARIS);
	public static final boolean MAC = (osType == OSType.OS_TYPE_MAC);
	public static final boolean FREEBSD = (osType == OSType.OS_TYPE_FREEBSD);
	public static final boolean LINUX = (osType == OSType.OS_TYPE_LINUX);
	public static final boolean OTHER = (osType == OSType.OS_TYPE_OTHER);

	public static final boolean PPC_64 = System.getProperties().getProperty("os.arch").contains("ppc64");



	/**
	 * Returns a File referencing a script with the given basename, inside the
	 * given parent directory. The file extension is inferred by platform:
	 * ".cmd" on Windows, or ".sh" otherwise.
	 * 
	 * @param parent
	 *            File parent directory
	 * @param basename
	 *            String script file basename
	 * @return File referencing the script in the directory
	 */
	public static File appendScriptExtension(File parent, String basename) {
		return new File(parent, appendScriptExtension(basename));
	}

	/**
	 * Returns a script file name with the given basename. The file extension is
	 * inferred by platform: ".cmd" on Windows, or ".sh" otherwise.
	 * 
	 * @param basename
	 *            String script file basename
	 * @return String script file name
	 */
	public static String appendScriptExtension(String basename) {
		return basename + (WINDOWS ? ".cmd" : ".sh");
	}

	/**
	 * Returns a command to run the given script. The script interpreter is
	 * inferred by platform: cmd on Windows or bash otherwise.
	 * 
	 * @param script
	 *            File script to run
	 * @return String[] command to run the script
	 */
	public static String[] getRunScriptCommand(File script) {
		String absolutePath = script.getAbsolutePath();
		return WINDOWS ? new String[] { "cmd", "/c", absolutePath } : new String[] { "/bin/bash", absolutePath };
	}

	/** Time after which the executing script would be timedout */
	protected long timeOutInterval = 0L;
	/** If or not script timed out */
	private AtomicBoolean timedOut;
	
	/** Token separator regex used to parse Shell tool outputs */
	public static final String TOKEN_SEPARATOR_REGEX = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

	private long interval; // refresh interval in msec
	private long lastTime; // last time the command was performed
	final private boolean redirectErrorStream; // merge stdout and stderr
	private Map<String, String> environment; // env for the command execution
	private File dir;
	private Process process; // sub process used to execute the command
	private int exitCode;

	/** If or not script finished executing */
	private volatile AtomicBoolean completed;

	public Shell() {
		this(0L);
	}

	public Shell(long interval) {
		this(interval, false);
	}

	/**
	 * @param interval
	 *            the minimum duration to wait before re-executing the command.
	 */
	public Shell(long interval, boolean redirectErrorStream) {
		this.interval = interval;
		this.lastTime = (interval < 0) ? 0 : -interval;
		this.redirectErrorStream = redirectErrorStream;
	}

	/**
	 * set the environment for the command
	 * 
	 * @param env
	 *            Mapping of environment variables
	 */
	protected void setEnvironment(Map<String, String> env) {
		this.environment = env;
	}

	/**
	 * set the working directory
	 * 
	 * @param dir
	 *            The directory where the command would be executed
	 */
	protected void setWorkingDirectory(File dir) {
		this.dir = dir;
	}

	/** check to see if a command needs to be executed and execute if needed */
	protected void run() throws IOException {
		if (lastTime + interval > Time.now())
			return;
		exitCode = 0; // reset for next run
		runCommand();
	}

	/** Run a command */
	private void runCommand() throws IOException {
		ProcessBuilder builder = new ProcessBuilder(getExecString());
		Timer timeOutTimer = null;
		ShellTimeoutTimerTask timeoutTimerTask = null;
		timedOut = new AtomicBoolean(false);
		completed = new AtomicBoolean(false);

		if (environment != null) {
			builder.environment().putAll(this.environment);
		}
		if (dir != null) {
			builder.directory(this.dir);
		}

		builder.redirectErrorStream(redirectErrorStream);

		if (Shell.WINDOWS) {
			synchronized (WindowsProcessLaunchLock) {
				// To workaround the race condition issue with child processes
				// inheriting unintended handles during process launch that can
				// lead to hangs on reading output and error streams, we
				// serialize process creation. More info available at:
				// http://support.microsoft.com/kb/315939
				process = builder.start();
			}
		} else {
			process = builder.start();
		}

		if (timeOutInterval > 0) {
			timeOutTimer = new Timer("Shell command timeout");
			timeoutTimerTask = new ShellTimeoutTimerTask(this);
			// One time scheduling.
			timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
		}
		final BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
		BufferedReader inReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		final StringBuffer errMsg = new StringBuffer();

		// read error and input streams as this would free up the buffers
		// free the error stream buffer
		Thread errThread = new Thread() {
			@Override
			public void run() {
				try {
					String line = errReader.readLine();
					while ((line != null) && !isInterrupted()) {
						errMsg.append(line);
						errMsg.append(System.getProperty("line.separator"));
						line = errReader.readLine();
					}
				} catch (IOException ioe) {
					LOG.warn("Error reading the error stream", ioe);
				}
			}
		};
		try {
			errThread.start();
		} catch (IllegalStateException ise) {
		}
		try {
			parseExecResult(inReader); // parse the output
			// clear the input stream buffer
			String line = inReader.readLine();
			while (line != null) {
				line = inReader.readLine();
			}
			// wait for the process to finish and check the exit code
			exitCode = process.waitFor();
			// make sure that the error thread exits
			joinThread(errThread);
			completed.set(true);
			// the timeout thread handling
			// taken care in finally block
			if (exitCode != 0) {
				throw new ExitCodeException(exitCode, errMsg.toString());
			}
		} catch (InterruptedException ie) {
			throw new IOException(ie.toString());
		} finally {
			if (timeOutTimer != null) {
				timeOutTimer.cancel();
			}
			// close the input stream
			try {
				// JDK 7 tries to automatically drain the input streams for us
				// when the process exits, but since close is not synchronized,
				// it creates a race if we close the stream first and the same
				// fd is recycled. the stream draining thread will attempt to
				// drain that fd!! it may block, OOM, or cause bizarre behavior
				// see: https://bugs.openjdk.java.net/browse/JDK-8024521
				// issue is fixed in build 7u60
				InputStream stdout = process.getInputStream();
				synchronized (stdout) {
					inReader.close();
				}
			} catch (IOException ioe) {
				LOG.warn("Error while closing the input stream", ioe);
			}
			if (!completed.get()) {
				errThread.interrupt();
				joinThread(errThread);
			}
			try {
				InputStream stderr = process.getErrorStream();
				synchronized (stderr) {
					errReader.close();
				}
			} catch (IOException ioe) {
				LOG.warn("Error while closing the error stream", ioe);
			}
			process.destroy();
			lastTime = Time.now();
		}
	}

	private static void joinThread(Thread t) {
		while (t.isAlive()) {
			try {
				t.join();
			} catch (InterruptedException ie) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("Interrupted while joining on: " + t, ie);
				}
				t.interrupt(); // propagate interrupt
			}
		}
	}

	/** return an array containing the command name & its parameters */
	protected abstract String[] getExecString();

	/** Parse the execution result */
	protected abstract void parseExecResult(BufferedReader lines) throws IOException;

	/**
	 * Get the environment variable
	 */
	public String getEnvironment(String env) {
		return environment.get(env);
	}

	/**
	 * get the current sub-process executing the given command
	 * 
	 * @return process executing the command
	 */
	public Process getProcess() {
		return process;
	}

	/**
	 * get the exit code
	 * 
	 * @return the exit code of the process
	 */
	public int getExitCode() {
		return exitCode;
	}


	/**
	 * To check if the passed script to shell command executor timed out or not.
	 * 
	 * @return if the script timed out.
	 */
	public boolean isTimedOut() {
		return timedOut.get();
	}

	/**
	 * Set if the command has timed out.
	 * 
	 */
	private void setTimedOut() {
		this.timedOut.set(true);
	}

	/**
	 * Static method to execute a shell command. Covers most of the simple cases
	 * without requiring the user to implement the <code>Shell</code> interface.
	 * 
	 * @param cmd
	 *            shell command to execute.
	 * @return the output of the executed command.
	 */
	public static String execCommand(String... cmd) throws IOException {
		return execCommand(null, cmd, 0L);
	}
	
    /**
     * Static method to execute a shell command. Covers most of the simple cases
     * without requiring the user to implement the <code>Shell</code> interface.
     * 
     * @param timeout time in milliseconds after which script should be marked timeout
     * @param cmd shell command to execute.
     * @return the output of the executed command.
     */
    public static String execCommand(long timeout, String... cmd) throws IOException {
        return execCommand(null, cmd, timeout);
    }

	/**
	 * Static method to execute a shell command. Covers most of the simple cases
	 * without requiring the user to implement the <code>Shell</code> interface.
	 * 
	 * @param env
	 *            the map of environment key=value
	 * @param cmd
	 *            shell command to execute.
	 * @param timeout
	 *            time in milliseconds after which script should be marked
	 *            timeout
	 * @return the output of the executed command.o
	 */

	public static String execCommand(Map<String, String> env, String[] cmd, long timeout) throws IOException {
		ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env, timeout);
		exec.execute();
		return exec.getOutput();
	}

	/**
	 * Static method to execute a shell command. Covers most of the simple cases
	 * without requiring the user to implement the <code>Shell</code> interface.
	 * 
	 * @param env
	 *            the map of environment key=value
	 * @param cmd
	 *            shell command to execute.
	 * @return the output of the executed command.
	 */
	public static String execCommand(Map<String, String> env, String... cmd) throws IOException {
		return execCommand(env, cmd, 0L);
	}

	/**
	 * Timer which is used to timeout scripts spawned off by shell.
	 */
	private static class ShellTimeoutTimerTask extends TimerTask {

		private Shell shell;

		public ShellTimeoutTimerTask(Shell shell) {
			this.shell = shell;
		}

		@Override
		public void run() {
			Process p = shell.getProcess();
			try {
				p.exitValue();
			} catch (Exception e) {
				// Process has not terminated.
				// So check if it has completed
				// if not just destroy it.
				if (p != null && !shell.completed.get()) {
					shell.setTimedOut();
					p.destroy();
				}
			}
		}
	}
}
