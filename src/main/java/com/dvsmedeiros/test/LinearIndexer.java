package com.dvsmedeiros.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

public class LinearIndexer extends Indexer {

	public static void main(String[] args) throws IOException, InterruptedException {

		long start = System.currentTimeMillis();

		List<String> lines = Files.readAllLines(Paths.get(DB_HOME.concat(FOLDER_SEPATOR).concat(FILE_NAME)));		
		StringTokenizer metaData = new StringTokenizer(lines.get(0), " ");
		int totalRows = Integer.parseInt(metaData.nextToken());
		int dimensions = metaData.countTokens();

		Map<Integer, Map<Integer, Set<Integer>>> dbIndexes = new HashMap<>();
		IntStream.range(0, dimensions).forEach(dimension -> {
			dbIndexes.put(++dimension, new HashMap<>());
		});
		
		int index = 1;
		for (String line : lines) {
			index(line, index++, dimensions, dbIndexes);
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Indexed in :" + (end - start) + "ms");
		
		start = System.currentTimeMillis();
		dbIndexes.keySet().forEach(dimensionId -> {
			Map<Integer, Set<Integer>> dimension = dbIndexes.get(dimensionId);
			System.out.println("write indexes for dimension: " + dimensionId);
			dimension.keySet().forEach(attr -> {
				try {
					write(dimension.get(attr), dimensionId, attr);
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
		});
		end = System.currentTimeMillis();
		System.out.println("Written in :" + (end - start) + "ms");
		
	}
}
