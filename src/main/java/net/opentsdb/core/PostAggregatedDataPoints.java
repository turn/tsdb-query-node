/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import net.opentsdb.meta.Annotation;

public class PostAggregatedDataPoints implements DataPoints {

	private final DataPoints baseDataPoints;
	private final DataPoint[] points;

	private String alias = null;

	public PostAggregatedDataPoints(DataPoints baseDataPoints, DataPoint[] points) {
		this.baseDataPoints = baseDataPoints;
		this.points = points;
	}

	/***
	 * Sort points in ascending order of timestamp, and create a {@code DataPoints} object
	 *
	 * @param baseDataPoints used to pick up tags, metric names etc;
	 * @param points to be sorted
	 * @return DataPoints impl where the data points are guaranteed to be sorted
	 */
	public static PostAggregatedDataPoints sortAndCreate(DataPoints baseDataPoints,
	                                                     MutableDataPoint[] points) {
		Preconditions.checkNotNull(points);
		Arrays.sort(points, TS_COMPARATOR);
		return new PostAggregatedDataPoints(baseDataPoints, points);
	}

	/***
	 * Sort points in ascending order of timestamp, and create a {@code DataPoints} object
	 *
	 * @param baseDataPoints used to pick up tags, metric names etc;
	 * @param points to be sorted
	 * @return DataPoints impl where the data points are guaranteed to be sorted
	 */
	public static PostAggregatedDataPoints sortAndCreate(DataPoints baseDataPoints,
	                                                     List<MutableDataPoint> points) {
		Preconditions.checkNotNull(points);
		MutableDataPoint[] pts = points.toArray(new MutableDataPoint[points.size()]);
		return sortAndCreate(baseDataPoints, pts);
	}

	public static Comparator<MutableDataPoint> TS_COMPARATOR = new Comparator<MutableDataPoint>() {
		@Override
		public int compare(MutableDataPoint o1, MutableDataPoint o2) {
			return (int) (o1.timestamp() - o2.timestamp());
		}
	};

	@Override
	public String metricName() {
		if (alias != null) return alias;
		else return baseDataPoints.metricName();
	}

	@Override
	public Deferred<String> metricNameAsync() {
		if (alias != null) return Deferred.fromResult(alias);
		return baseDataPoints.metricNameAsync();
	}

	@Override
	public Map<String, String> getTags() {
		if (alias != null) return Maps.newHashMap();
		else return baseDataPoints.getTags();
	}

	@Override
	public Deferred<Map<String, String>> getTagsAsync() {
		Map<String, String> def = new HashMap<String, String>();
		if (alias != null) return Deferred.fromResult(def);
		return baseDataPoints.getTagsAsync();
	}

	@Override
	public List<String> getAggregatedTags() {
		return baseDataPoints.getAggregatedTags();
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	@Override
	public Deferred<List<String>> getAggregatedTagsAsync() {
		return baseDataPoints.getAggregatedTagsAsync();
	}

	@Override
	public List<String> getTSUIDs() {
		return baseDataPoints.getTSUIDs();
	}

	@Override
	public List<Annotation> getAnnotations() {
		return baseDataPoints.getAnnotations();
	}

	@Override
	public int size() {
		return points.length;
	}

	@Override
	public int aggregatedSize() {
		return points.length;
	}

	@Override
	public SeekableViewImpl iterator() {
		return new SeekableViewImpl(points);
	}

	@Override
	public long timestamp(int i) {
		return points[i].timestamp();
	}

	@Override
	public boolean isInteger(int i) {
		return points[i].isInteger();
	}

	@Override
	public long longValue(int i) {
		return points[i].longValue();
	}

	@Override
	public double doubleValue(int i) {
		return points[i].doubleValue();
	}

	public static PostAggregatedDataPoints[] fromArray(DataPoints[] base) {
		Preconditions.checkNotNull(base);
		PostAggregatedDataPoints[] results = new PostAggregatedDataPoints[base.length];
		for (int i=0; i<results.length; i++) {
			ArrayList<MutableDataPoint> mps = new ArrayList<MutableDataPoint>();
			for (DataPoint dp: base[i]) {
				mps.add(MutableDataPoint.fromPoint(dp));
			}
			results[i] = sortAndCreate(base[i], mps);
		}

		return results;
	}

	static class SeekableViewImpl implements SeekableView {

		private int pos = 0;
		private final DataPoint[] dps;

		public SeekableViewImpl(DataPoint[] dps) {
			this.dps = dps;
		}

		@Override
		public boolean hasNext() {
			return pos < dps.length;
		}

		@Override
		public DataPoint next() {
			if (hasNext()) {
				return dps[pos++];
			} else {
				throw new NoSuchElementException("tsdb uses exceptions to determine end of iterators");
			}
		}

		@Override
		public void remove() {
			throw new RuntimeException("Not supported exception");
		}

		@Override
		public void seek(long timestamp) {
			for (int i = pos; i < dps.length; i++) {
				if (dps[i].timestamp() >= timestamp) {
					break;
				} else {
					pos++;
				}
			}
		}

		public int current() {
			return pos;
		}

		public DataPoint currentPoint() {
			return dps[pos];
		}
	}

	public static void main(String[] args) {
		MutableDataPoint[] points = new MutableDataPoint[] {
				MutableDataPoint.ofLongValue(10, 2),
				MutableDataPoint.ofLongValue(5, 2),
				MutableDataPoint.ofLongValue(13, 2)
		};

		PostAggregatedDataPoints agg = PostAggregatedDataPoints.sortAndCreate(null, points);
		for (int i=0; i<agg.size(); i++) {
			System.out.println(agg.timestamp(i) + " " + agg.longValue(i));
		}
	}

}
