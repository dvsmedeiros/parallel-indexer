package com.dvsmedeiros.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.gson.Gson;
import com.jramoyo.io.IndexedFileReader;

public class ParallelIndexer extends Indexer {

	public static void main(String[] args) throws IOException, InterruptedException {

		long start = System.currentTimeMillis();

		File file = new File(DB_HOME.concat(FOLDER_SEPATOR).concat(FILE_NAME));
		IndexedFileReader reader = new IndexedFileReader(file);

		StringTokenizer metaData = readMetaData(file);

		int totalRows = Integer.parseInt(metaData.nextToken());
		int dimensions = metaData.countTokens();
		int perPage = 5000;

		BlockingQueue<Page> toProcess = getPages(totalRows, perPage);
		//BlockingQueue<Map<Integer, Map<Integer, Set<Integer>>>> toSave = new LinkedBlockingQueue<>();
		BlockingQueue<Integer> toSave = new LinkedBlockingQueue<>();
		
		ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
		
		Map<Integer, Map<Integer, Set<Integer>>> dbIndexes = new HashMap<>();
		IntStream.range(0, dimensions).forEach(dimension -> {
			dbIndexes.put(++dimension, new HashMap<>());
			try {
				toSave.put(dimension);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		
		Map<String, ThreadStatistic> map = new ConcurrentHashMap<>();
		IntStream.range(0, NTHREADS - 1).forEach(i -> {
			String key = THREAD_NAME + ++i;
			map.put(key, new ThreadStatistic());
			executor.submit(() -> {
				while (!toProcess.isEmpty()) {
					try {
						long startExec = System.currentTimeMillis();
						Page page = toProcess.take();
						SortedMap<Integer, String> tuples;
						tuples = read(reader, page.getStart(), page.getEnd());
						tuples.keySet().forEach(index -> {
							//try {
								if (index == 1) return;
								index(tuples.get(index), index, dimensions, dbIndexes);
								//Map<Integer, Map<Integer, Set<Integer>>> indexedTuple = index(tuples.get(index), index, dimensions);
								//toSave.put(indexedTuple);
							///} catch (InterruptedException e) {
							//	e.printStackTrace();
							//}
						});
						
						long endExec = System.currentTimeMillis();
						long exec = endExec - startExec;
						
						ThreadStatistic threadStatistic = map.get(Thread.currentThread().getName());
						threadStatistic.getExecutions().incrementAndGet();						
						threadStatistic.setTimeExecution(threadStatistic.getTimeExecution() + exec);
						
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		});
		
		executor.shutdown();
		executor.awaitTermination(24L, TimeUnit.HOURS);
		reader.close();
		
		System.out.println(new Gson().toJson(map));
		long end = System.currentTimeMillis();
		System.out.println("Indexed in :" + (end - start) + "ms");
		
		ExecutorService writeExecutor = Executors.newFixedThreadPool(NTHREADS);
		
		start = System.currentTimeMillis();
		/*
		IntStream.range(0, NTHREADS).forEach(i -> {
			writeExecutor.submit(() -> {
				try {
					Map<Integer, Map<Integer, Set<Integer>>> indexedTuple = toSave.take();
					indexedTuple.keySet().forEach(dimensionId -> {
						Map<Integer, Set<Integer>> dimension = indexedTuple.get(dimensionId);
						System.out.println(Thread.currentThread().getName() + " write indexes for dimension: " + dimensionId);
						dimension.keySet().forEach(attr -> {
							try {
								write(dimension.get(attr), dimensionId, attr);
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
					});
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
		});
		*/
		IntStream.range(0, NTHREADS).forEach(i -> {
			writeExecutor.submit(() -> {
				while (!toSave.isEmpty() ) {
					Integer dimensionId;
					try {
						dimensionId = toSave.take();
						Map<Integer, Set<Integer>> dimension = dbIndexes.get(dimensionId);
						System.out.println(Thread.currentThread().getName() + " write indexes for dimension: " + dimensionId);
						dimension.keySet().forEach(attr -> {
							try {
								write(dimension.get(attr), dimensionId, attr);
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		});
		
		writeExecutor.shutdown();
		writeExecutor.awaitTermination(24L, TimeUnit.HOURS);
		end = System.currentTimeMillis();
		System.out.println("Written in :" + (end - start) + "ms");
		
	}
	
	public static final SortedMap<Integer, String> read(IndexedFileReader reader, int start, int end) throws IOException {
		return reader.readLines(start, end);
	}

	public static final StringTokenizer readMetaData(File file) throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader(file));
		String metaData = bf.readLine();
		bf.close();
		return new StringTokenizer(metaData, " ");
	}

	public static Page getPage(int page, int perPage) {
		return new Page(page, perPage);
	}

	public static BlockingQueue<Page> getPages(int size, int perPage) {
		BlockingQueue<Page> queue = new LinkedBlockingQueue<>();
		int pages = (int) Math.ceil(size / perPage);
		IntStream.range(0, pages).forEach(page -> {
			try {
				queue.put(getPage(page, perPage));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		return queue;
	}
	
}
