package com.hadoop.plat.util.shell;

import java.io.IOException;

/**
 * This is an IOException with exit code added.
 */
public class ExitCodeException extends IOException {

	/** serialVersionUID */
	private static final long serialVersionUID = 3780539629861132616L;

	private final int exitCode;

	public ExitCodeException(int exitCode, String message) {
		super(message);
		this.exitCode = exitCode;
	}

	public int getExitCode() {
		return exitCode;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ExitCodeException ");
		sb.append("exitCode=").append(exitCode).append(": ");
		sb.append(super.getMessage());
		return sb.toString();
	}
}
