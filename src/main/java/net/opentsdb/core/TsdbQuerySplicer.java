/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
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
		List<ListenableFuture<DataPoints[]>> resultFutureList = Lists.newArrayList();
		for (final TsdbQuery splice : splices) {
			ListenableFuture<DataPoints[]> aggFuture =
					Futures.transform(POOL.submit(new SpliceDataFetch(splice)), FETCH_AND_AGG);
			resultFutureList.add(aggFuture);
		}

		ListenableFuture<List<DataPoints[]>> resultListFuture = Futures.allAsList(resultFutureList);

		DataPoints[] joinedResults = join(resultListFuture);

		LOG.info("# of Joined results = " + joinedResults.length);

		return joinedResults;
	}

	private DataPoints[] join(ListenableFuture<List<DataPoints[]>> resultListFuture) {
		try {
			List<DataPoints[]> results = resultListFuture.get();
			return results.get(0);
		} catch (InterruptedException e) {
			LOG.error("Thread Interrupted", e);
		} catch (ExecutionException e) {
			LOG.error("Error in execution", e);
		}

		return new DataPoints[0];
	}

	static AsyncFunction<Result, DataPoints[]> FETCH_AND_AGG = new AsyncFunction<Result, DataPoints[]>() {
		@Override
		public ListenableFuture<DataPoints[]> apply(Result result) throws Exception {
			return POOL.submit(new AggregateDataPoints(result.datapoints, result.splice));
		}
	};

	static class AggregateDataPoints implements Callable<DataPoints[]> {
		private final DataPoints[] rawData;
		private final TsdbQuery splice;

		public AggregateDataPoints(DataPoints[] rawData, TsdbQuery splice) {
			this.rawData = rawData;
			this.splice = splice;
		}

		@Override
		public DataPoints[] call() {
			DataPoints[] processedPoints = new PostAggregatedDataPoints[rawData.length];
			for (int ix = 0; ix < rawData.length; ix++) {
				DataPoints points = rawData[ix];

				LOG.info("For metric={}, tags={} aggTags={}", points.metricName(), points.getTags(), points.getAggregatedTags());

				List<DataPoint> filteredPoints = Lists.newArrayList();
				for (DataPoint point: points) {
					long ts = point.timestamp();
					if (ts >= (splice.getStartTime() * 1000) && ts <= (splice.getEndTime() * 1000)) {
						MutableDataPoint dp;
						if (point.isInteger()) {
							dp = MutableDataPoint.ofLongValue(ts, point.longValue());
						} else {
							dp = MutableDataPoint.ofDoubleValue(ts, point.doubleValue());
						}
						LOG.info("Found point {}", dp);
						filteredPoints.add(dp);
					}
				}

				processedPoints[ix] = new PostAggregatedDataPoints(points, filteredPoints.toArray(new DataPoint[filteredPoints.size()]));
			}
			return processedPoints;
		}
	}

	static class SpliceDataFetch implements Callable<Result> {

		private final TsdbQuery splice;

		public SpliceDataFetch(TsdbQuery splice) {
			this.splice = splice;
		}

		@Override
		public Result call() throws Exception {
			DataPoints[] points = splice.runWithoutSplice().joinUninterruptibly();
			LOG.info("Found {} datapoint collections", points != null ? points.length : "null");
			return new Result(splice, points);
		}
	}

	static class Result {

		TsdbQuery splice;
		DataPoints[] datapoints;

		public Result(TsdbQuery splice, DataPoints[] datapoints) {
			this.splice = splice;
			this.datapoints = datapoints;
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
}
