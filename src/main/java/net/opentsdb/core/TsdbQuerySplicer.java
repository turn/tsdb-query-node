/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsdbQuerySplicer {

	static ListeningExecutorService POOL = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

	private static final Logger LOG = LoggerFactory.getLogger(TsdbQuery.class);

	private final TSDB tsdb;
	private final TsdbQuery query;

	public TsdbQuerySplicer(TSDB tsdb, TsdbQuery query) {
		this.tsdb = tsdb;
		this.query = query;
	}

	public DataPoints[] execute() {
		long startTime = query.getStartTime();
		// down cast to seconds if we have a query in ms
		if ((startTime & Const.SECOND_MASK) != 0) {
			startTime /= 1000;
		}

		long endTime = query.getEndTime();
		if ((endTime & Const.SECOND_MASK) != 0) {
			endTime /= 1000;
		}

		LOG.info("Parallelizing query with startTime={}, endTime={}", startTime, endTime);

		List<TsdbQuery> splices = spliceQuery(startTime, endTime);

		// if we have too few splices. Run it in current thread.
		if (splices.size() <= 2) {
			return runWithoutSplicing();
		}

		// if we have sufficient splices, execute them in parallel.
		List<ListenableFuture<Result>> resultFutureList = Lists.newArrayList();
		for (final TsdbQuery splice : splices) {
			ListenableFuture<Result> aggFuture =
					Futures.transform(POOL.submit(new SpliceFetch(splice)), FETCH_AND_AGG);
			resultFutureList.add(aggFuture);
		}

		ListenableFuture<List<Result>> resultListFuture = Futures.allAsList(resultFutureList);

		DataPoints[] joinedResults = join(resultListFuture);
		LOG.info("# of Joined results = " + joinedResults.length);

		return joinedResults;
	}

	public DataPoints[] join(ListenableFuture<List<Result>> resultListFuture) {
		try {
			List<Result> results = resultListFuture.get();
			List<PostAggregatedDataPoints[]> rdp = new ArrayList<PostAggregatedDataPoints[]>();
			for (Result r: results) {
				if (r.datapoints instanceof PostAggregatedDataPoints[]) {
					rdp.add(((PostAggregatedDataPoints[]) r.datapoints));
				} else {
					LOG.error("Data point format conversion at join stage.");
					rdp.add(PostAggregatedDataPoints.fromArray(r.datapoints));
				}
			}

			AggregateAppender appender = new AggregateAppender(rdp);
			List<PostAggregatedDataPoints> result = appender.append(appender.orderAggregates());
			LOG.info("Final result has {} aggregates", result.size());
			return result.toArray(new DataPoints[result.size()]);
		} catch (InterruptedException e) {
			LOG.error("Thread Interrupted", e);
		} catch (ExecutionException e) {
			LOG.error("Error in execution", e);
		}

		return new DataPoints[0];
	}

	static class AggregateAppender {
		private final List<PostAggregatedDataPoints[]> rdp;
		private final int numAggregates;

		public AggregateAppender(List<PostAggregatedDataPoints[]> rdp) {
			Preconditions.checkNotNull(rdp);

			if (rdp.size() == 0) {
				this.rdp = new ArrayList<PostAggregatedDataPoints[]>();
				this.rdp.add(new PostAggregatedDataPoints[]{});
			} else {
				this.rdp = rdp;
			}

			numAggregates = rdp.get(0).length;
			for (PostAggregatedDataPoints[] aggs: rdp) {
				if (aggs.length != numAggregates) {
					throw new IllegalArgumentException(
							"not enough params " + aggs.length + ", " + numAggregates);
				}
			}
		}

		public List<List<PostAggregatedDataPoints>> orderAggregates() {
			List<List<PostAggregatedDataPoints>> flattened = new ArrayList<List<PostAggregatedDataPoints>>();
			if (rdp.size() == 0) {
				return flattened;
			}

			if (rdp.size() == 1) {
				flattened.add(Arrays.asList(rdp.get(0)));
			}

			PostAggregatedDataPoints[] first = rdp.get(0);
			for (PostAggregatedDataPoints agg: first) {
				ArrayList<PostAggregatedDataPoints> row = new ArrayList<PostAggregatedDataPoints>();
				row.add(agg);
				for (int i = 1; i < rdp.size(); i++) {
					boolean found = false;
					for (PostAggregatedDataPoints cell: rdp.get(i)) {
						if (signatureMatches(cell, agg)) {
							row.add(cell);
							found = true;
							break;
						}
					}

					if (!found) {
						LOG.error("Should not be here. No match for metric with signature: {}", signature(agg));
					}

				}
				flattened.add(row);
			}

			return flattened;
		}

		private String signature(PostAggregatedDataPoints agg) {
			return "metric=" + agg.metricName()
					+ ", tags=" + agg.getTags()
					+ ", aggregatedTags=" + agg.getAggregatedTags();
		}

		private boolean signatureMatches(PostAggregatedDataPoints first,
		                                 PostAggregatedDataPoints cell) {
			String metricName = first.metricName();
			List<String> aggTags = first.getAggregatedTags();
			Map<String, String> tags = first.getTags();

			if (!metricName.equals(cell.metricName())) {
				return false;
			}

			if (aggTags.size() != cell.getAggregatedTags().size()) {
				return false;
			}

			if (!aggTags.containsAll(cell.getAggregatedTags())) {
				return false;
			}

			if (tags.size() != cell.getTags().size()) {
				return false;
			}

			for (Map.Entry<String, String> e: tags.entrySet()) {
				String firstVal = tags.get(e.getKey());
				String cellVal = cell.getTags().get(e.getKey());
				if (!firstVal.equals(cellVal)) {
					return false;
				}
			}

			return true;
		}

		public List<PostAggregatedDataPoints> append(List<List<PostAggregatedDataPoints>> table) {
			if (table == null || table.size() == 0) {
				return Lists.newArrayList();
			}

			List<PostAggregatedDataPoints> individualRows = Lists.newArrayList();
			for (List<PostAggregatedDataPoints> columns: table) {
				PostAggregatedDataPoints compact = appendCols(columns);
				individualRows.add(compact);
			}

			return individualRows;
		}

		public PostAggregatedDataPoints appendCols(List<PostAggregatedDataPoints> columns) {
			if (columns.size() <= 1) {
				throw new RuntimeException("Must be atleast 2 columns to append");
			}

			Map<Long, DataPoint> dedupMap = new TreeMap<Long, DataPoint>();
			for (PostAggregatedDataPoints col: columns) {
				for (DataPoint dp: col) {
					dedupMap.put(dp.timestamp(), dp);
				}
			}

			MutableDataPoint[] mdps = new MutableDataPoint[dedupMap.size()];
			int i = 0;
			for (Map.Entry<Long, DataPoint> e: dedupMap.entrySet()) {
				if (e.getValue() instanceof MutableDataPoint) {
					mdps[i++] = (MutableDataPoint) e.getValue();
				} else {
					mdps[i++] = MutableDataPoint.fromPoint(e.getValue());
				}
			}

			// there must be atleast two columns  for this method to be called
			return new PostAggregatedDataPoints(columns.get(0), mdps);
		}

		static Comparator<PostAggregatedDataPoints.SeekableViewImpl> SEEKVIEW_COMPARATOR =
				new Comparator<PostAggregatedDataPoints.SeekableViewImpl>() {
					@Override
					public int compare(PostAggregatedDataPoints.SeekableViewImpl o1,
					                   PostAggregatedDataPoints.SeekableViewImpl o2) {
						return (int) (o1.currentPoint().timestamp() - o2.currentPoint().timestamp());
					}
				};

	}

	static AsyncFunction<Result, Result> FETCH_AND_AGG = new AsyncFunction<Result, Result>() {
		@Override
		public ListenableFuture<Result> apply(Result intermediate) throws Exception {
			return POOL.submit(new SpliceAggregator(intermediate));
		}
	};

	static class SpliceAggregator implements Callable<Result> {
		private final Result intermediate;

		public SpliceAggregator(Result intermediate) {
			this.intermediate = intermediate;
		}

		@Override
		public Result call() {
			long start = System.nanoTime();

			DataPoints[] rawData = intermediate.datapoints;
			TsdbQuery splice = intermediate.splice;

			DataPoints[] processedPoints = new PostAggregatedDataPoints[rawData.length];
			for (int ix = 0; ix < rawData.length; ix++) {
				DataPoints points = rawData[ix];

				LOG.info("For metric={}, tags={} aggTags={}",
						points.metricName(),
						points.getTags(),
						points.getAggregatedTags());

				List<MutableDataPoint> filtered = Lists.newArrayList();
				for (DataPoint point: points) {
					long ts = point.timestamp() / 1000; // timestamp in seconds
					if (ts >= splice.getStartTime() && ts <= splice.getEndTime()) {
						MutableDataPoint dp = MutableDataPoint.fromPoint(point);
						LOG.info("Found point {}", dp);
						filtered.add(dp);
					}
				}

				processedPoints[ix] = PostAggregatedDataPoints.sortAndCreate(points, filtered);
			}

			long aggTimeInNanos = System.nanoTime() - start;
			return new Result(splice, processedPoints, intermediate.spliceFetchTime, aggTimeInNanos);
		}
	}

	static class SpliceFetch implements Callable<Result> {

		private final TsdbQuery splice;

		public SpliceFetch(TsdbQuery splice) {
			this.splice = splice;
		}

		@Override
		public Result call() throws Exception {
			long start = System.nanoTime();

			DataPoints[] points = splice.runWithoutSplice().joinUninterruptibly();
			LOG.info("Found {} datapoint collections", points == null ? "null" : points.length);

			long fetchTimeInNanos = System.nanoTime() - start;
			return new Result(splice, points, fetchTimeInNanos);
		}
	}

	static class Result {

		final TsdbQuery splice;
		final DataPoints[] datapoints;

		// for instrumentation (numbers in ns)
		final long spliceFetchTime;
		final long spliceAggregationTime;

		public Result(TsdbQuery splice, DataPoints[] datapoints, long spliceFetchTime) {
			this(splice, datapoints, spliceFetchTime, -1);
		}

		public Result(TsdbQuery splice, DataPoints[] datapoints, long spliceFetchTime,
		              long spliceAggregationTime) {
			this.splice = splice;
			this.datapoints = datapoints;
			this.spliceFetchTime = spliceFetchTime;
			this.spliceAggregationTime = spliceAggregationTime;
		}
	}


	private DataPoints[] runWithoutSplicing() {
		try {
			return query.runWithoutSplice().joinUninterruptibly();
		} catch (Exception e) {
			LOG.error("Could not execute query" + query, e);
		}

		return new DataPoints[0];
	}

	public List<TsdbQuery> spliceQuery(long startTime, long endTime) {
		final long bucket_size = tsdb.getConfig().parallel_scan_bucket_size();

		List<TsdbQuery> splices = new ArrayList<TsdbQuery>();
		long end = startTime - (startTime % bucket_size) + bucket_size;
		splices.add(TsdbQuery.spliceOf(query, startTime, end));
		LOG.info("First interval is {} to {}", startTime, end);

		while (end + bucket_size < endTime) {
			TsdbQuery splice = TsdbQuery.spliceOf(query, end, end + bucket_size);
			splices.add(splice);
			end = end + bucket_size;
			LOG.info("Add interval# {} from {} to {}", splices.size(),
					splice.getStartTime(),
					splice.getEndTime());
		}

		splices.add(TsdbQuery.spliceOf(query, end, endTime));
		LOG.info("Last interval is {} to {}", end, endTime);

		return splices;
	}

	static class FakeDataPoints extends PostAggregatedDataPoints {
		final String metric;
		final List<String> aggtags;
		final Map<String, String> tags;

		public FakeDataPoints(String metric, List<String> aggtags, Map<String, String> tags, MutableDataPoint[] mdps) {
			super(null, mdps);
			this.metric = metric;
			this.aggtags = aggtags;
			this.tags = tags;
		}

		@Override
		public String metricName() {
			return metric;
		}

		@Override
		public Map<String, String> getTags() {
			return tags;
		}

		@Override
		public List<String> getAggregatedTags() {
			return aggtags;
		}

	}

	static PostAggregatedDataPoints fake(String metric,
	                                     List<String> aggTags,
	                                     Map<String, String> tags,
	                                     long offset) {
		MutableDataPoint[] mdps = new MutableDataPoint[10];
		for (int i = 0; i < mdps.length; i++) {
			mdps[i] = MutableDataPoint.ofLongValue(offset + (100 * i), i * i);
		}
		List<MutableDataPoint> list = Arrays.asList(mdps);
		Collections.shuffle(list);

		return new FakeDataPoints(metric, aggTags, tags, list.toArray(new MutableDataPoint[list.size()]));
	}

	public static void main(String[] args) {
		List<PostAggregatedDataPoints[]> results = Lists.newArrayList();

		List<PostAggregatedDataPoints> sample = Lists.newArrayList();
		String[] domains = new String[]{"sjc2", "atl1", "atl2", "ams1", "hkg1"};
		for (int i=0; i<5; i++) {
			Map<String, String> tags = Maps.newHashMap();
			tags.put("domain", domains[i]);
			List<String> aggTags = Lists.newArrayList();
			for (int j=i; j<5; j++) {
				aggTags.add("ag" + i);
			}
			sample.add(fake("metric", aggTags, tags, i * 1000));
		}

		for (int i=0; i<3; i++) {
			Collections.shuffle(sample);
			results.add(sample.toArray(new PostAggregatedDataPoints[sample.size()]));
		}

		AggregateAppender appender = new AggregateAppender(results);
		List<PostAggregatedDataPoints> compact = appender.append(appender.orderAggregates());
		LOG.info("Final result has {} aggregates", compact.size());

		for (PostAggregatedDataPoints padp: compact) {
			String line = "";
			LOG.info(padp.metricName() + " " + padp.getTags() + " " + padp.getAggregatedTags());
			for (DataPoint dp: padp) {
				line += "(" + dp.timestamp() + "," + dp.longValue() + ")";
			}
			LOG.info("line=" + line);
		}
	}
}
