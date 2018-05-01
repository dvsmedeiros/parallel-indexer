package com.dvsmedeiros.test;

public class Page {

	public Page(int current, int perPage) {
		this.current = current;
		this.perPage = perPage;
	}

	int current;
	int perPage;

	public int getStart() {
		return current * perPage + 1;
	}

	public int getEnd() {
		return getStart() + perPage - 1;
	}

	@Override
	public String toString() {
		return "Page [current=" + current + ", perPage=" + perPage + ", start=" + getStart() + ", end=" + getEnd()
				+ "]";
	}

}
