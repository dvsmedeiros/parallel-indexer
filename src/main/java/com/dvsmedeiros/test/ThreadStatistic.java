package com.dvsmedeiros.test;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadStatistic {

	private long timeExecution;
	private AtomicInteger executions = new AtomicInteger(0);

	public long getTimeExecution() {
		return timeExecution;
	}

	public void setTimeExecution(long timeExecution) {
		this.timeExecution = timeExecution;
	}

	public AtomicInteger getExecutions() {
		return executions;
	}

	public void setExecutions(AtomicInteger executions) {
		this.executions = executions;
	}

}
