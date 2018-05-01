package com.dvsmedeiros.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

public class Indexer {
	
	protected transient static final int NTHREADS = 5;
	protected transient static final String FILE_NAME = "D12_T1M_S0_C1K_100";
	protected transient static final String DB_HOME = "/Users/dvsmedeiros/dev/temp";
	protected transient static final String DB_NAME = "DB";
	protected transient static final String FOLDER_SEPATOR = "/";
	protected transient static final String IDX = ".idx";
	protected transient static final String THREAD_NAME = "pool-1-thread-";
	
	protected static void write(Set<Integer> indexes, Integer dimension, Integer attr) throws IOException {
		File indexFile = new File(getFullPath(dimension, attr));
		indexFile.getParentFile().mkdirs();
		FileWriter writer = new FileWriter(indexFile);
		indexes.forEach(index -> {
			try {
				writer.write(index.toString());
				writer.write("\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		writer.close();
	}

	protected static String getFullPath(int dimension, int attr) {
		StringBuilder sb = new StringBuilder();
		sb.append(getPath());
		sb.append(FOLDER_SEPATOR);
		sb.append(dimension);
		sb.append(FOLDER_SEPATOR);
		sb.append(attr);
		sb.append(FOLDER_SEPATOR);
		sb.append(attr);
		sb.append(IDX);
		return sb.toString();
	}

	protected static String getPath() {
		StringBuilder sb = new StringBuilder();
		sb.append(DB_HOME);
		sb.append(FOLDER_SEPATOR);
		sb.append(DB_NAME);
		return sb.toString();
	}

	protected static void index(String line, int index, int dimensions, Map<Integer, Map<Integer, Set<Integer>>> allIndexes) {
		StringTokenizer tuple = new StringTokenizer(line);
		IntStream.range(0, dimensions).forEach(dimensionId -> {
			Map<Integer, Set<Integer>> dimension = allIndexes.get(++dimensionId);
			int attr = Integer.parseInt(tuple.nextToken());
			if (!dimension.containsKey(attr)) {
				dimension.put(attr, new HashSet<>());
			}
			dimension.get(attr).add(index);			
		});
	}
	
	protected static Map<Integer, Map<Integer, Set<Integer>>> index(String line, int index, int dimensions) {
		System.out.println("INDEX: " + index + " LINE: " + line);
		StringTokenizer tuple = new StringTokenizer(line);
		Map<Integer, Map<Integer, Set<Integer>>> indexedTuple = new HashMap<>();
		IntStream.range(0, dimensions).forEach(dimensionId -> {
			Map<Integer, Set<Integer>> dimension = new HashMap<>();
			indexedTuple.put(dimensionId, dimension);
			int attr = Integer.parseInt(tuple.nextToken());
			if (!dimension.containsKey(attr)) {
				dimension.put(attr, new HashSet<>());
			}
			dimension.get(attr).add(index);			
		});
		return indexedTuple;
	}
	
}
