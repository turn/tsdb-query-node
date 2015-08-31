// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.Date;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

	private static final Logger LOG =
			LoggerFactory.getLogger(Downsampler.class);

	/**
	 * Function to use for downsampling.
	 */
	private final Aggregator downsampler;
	/**
	 * Iterator to iterate the values of the current interval.
	 */
	private final ValuesInInterval values_in_interval;

	/**
	 * Set to true if the {@link #values_in_interval} represents a monotonically increasing counter
	 */
	private final boolean isCounter;

	/**
	 * All values returned by {@link #next()} must be greater than base_value. Default = 0;
	 */
	private double base_value;

	/**
	 * The last value returned by the {@link #next()} method
	 */
	private Double last_value;

	/**
	 * Last normalized timestamp
	 */
	private long timestamp;
	/**
	 * Last value as a double
	 */
	private double value;

	/**
	 * Ctor.
	 *
	 * @param source      The iterator to access the underlying data.
	 * @param interval_ms The interval in milli seconds wanted between each data
	 *                    point.
	 * @param downsampler The downsampling function to use.
	 */
	Downsampler(final SeekableView source,
	            final long interval_ms,
	            final Aggregator downsampler) {
		this(source, interval_ms, downsampler, false);
	}

	/**
	 * Ctor.
	 *
	 * @param source      The iterator to access the underlying data.
	 * @param interval_ms The interval in milli seconds wanted between each data
	 *                    point.
	 * @param downsampler The downsampling function to use.
	 * @param isCounter   set to true if we expect source to represent a monotonically increasing
	 *                    function. For any drops in this value (for example, during service roll)
	 *                    the numbers after the drop will be added to the last max value seen till
	 *                    then. For multiple drops, the base value added to each post-drop data
	 *                    point will be the sum of the max values seen just before the drop.
	 */
	Downsampler(final SeekableView source,
	            final long interval_ms,
	            final Aggregator downsampler, boolean isCounter) {
		this.values_in_interval = new ValuesInInterval(source, interval_ms);
		this.downsampler = downsampler;
		this.isCounter = isCounter;
	}

	// ------------------ //
	// Iterator interface //
	// ------------------ //

	long totalDownsampleTime = 0;

	public boolean hasNext() {
		long hasNextStartTime = System.nanoTime();
		boolean b = values_in_interval.hasNextValue();
		totalDownsampleTime += System.nanoTime() - hasNextStartTime;
		return b;
	}

	public long totalTime() {
		return totalDownsampleTime;
	}

	public DataPoint next() {
		long hasNextStartTime = System.nanoTime();
		if (hasNext()) {
			last_value = value;
			value = downsampler.runDouble(values_in_interval);
			timestamp = values_in_interval.getIntervalTimestamp();
			values_in_interval.moveToNextInterval();
			totalDownsampleTime += System.nanoTime() - hasNextStartTime;
			return this;
		}
		throw new NoSuchElementException("no more data points in " + this);
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	// ---------------------- //
	// SeekableView interface //
	// ---------------------- //

	public void seek(final long timestamp) {
		values_in_interval.seekInterval(timestamp);
	}

	@Override
	public String toString() {
		final StringBuilder buf = new StringBuilder();
		buf.append("Downsampler: ")
				.append("interval_ms=").append(values_in_interval.interval_ms)
				.append(", downsampler=").append(downsampler)
				.append(", current data=(timestamp=").append(timestamp)
				.append(", value=").append(value)
				.append("), values_in_interval=").append(values_in_interval);
		return buf.toString();
	}

	public long timestamp() {
		return timestamp;
	}

	public boolean isInteger() {
		return false;
	}

	public long longValue() {
		throw new ClassCastException("Downsampled values are doubles");
	}

	public double doubleValue() {
		return base_lift();
	}

	public double toDouble() {
		return base_lift();
	}

	public double base_lift() {
		if (isCounter) {
			if (last_value > value) {
				base_value += last_value;
			}
			LOG.info("Returning Lifted Value: {}, {}, {}", new Date(timestamp), base_value, base_value + value);
			return base_value + value;
		} else {
			LOG.info("Returning: {}, {}", new Date(timestamp), value);
			return value;
		}
	}


	/**
	 * Iterates source values for an interval.
	 */
	private static class ValuesInInterval implements Aggregator.Doubles {

		/**
		 * The iterator of original source values.
		 */
		private final SeekableView source;
		/**
		 * The sampling interval in milliseconds.
		 */
		private final long interval_ms;
		/**
		 * The end of the current interval.
		 */
		private long timestamp_end_interval = Long.MIN_VALUE;
		/**
		 * True if the last value was successfully extracted from the source.
		 */
		private boolean has_next_value_from_source = false;
		/**
		 * The last data point extracted from the source.
		 */
		private DataPoint next_dp = null;

		/**
		 * True if it is initialized for iterating intervals.
		 */
		private boolean initialized = false;

		/**
		 * Constructor.
		 *
		 * @param source      The iterator to access the underlying data.
		 * @param interval_ms Downsampling interval.
		 */
		ValuesInInterval(final SeekableView source, final long interval_ms) {
			this.source = source;
			this.interval_ms = interval_ms;
			this.timestamp_end_interval = interval_ms;
		}

		/**
		 * Initializes to iterate intervals.
		 */
		private void initializeIfNotDone() {
			// NOTE: Delay initialization is required to not access any data point
			// from the source until a user requests it explicitly to avoid the severe
			// performance penalty by accessing the unnecessary first data of a span.
			if (!initialized) {
				initialized = true;
				moveToNextValue();
				resetEndOfInterval();
			}
		}

		/**
		 * Extracts the next value from the source.
		 */
		private void moveToNextValue() {
			if (source.hasNext()) {
				has_next_value_from_source = true;
				next_dp = source.next();
			} else {
				has_next_value_from_source = false;
			}
		}

		/**
		 * Resets the current interval with the interval of the timestamp of
		 * the next value read from source. It is the first value of the next
		 * interval.
		 */
		private void resetEndOfInterval() {
			if (has_next_value_from_source) {
				// Sets the end of the interval of the timestamp.
				timestamp_end_interval = alignTimestamp(next_dp.timestamp()) +
						interval_ms;
			}
		}

		/**
		 * Moves to the next available interval.
		 */
		void moveToNextInterval() {
			initializeIfNotDone();
			resetEndOfInterval();
		}

		/**
		 * Advances the interval iterator to the given timestamp.
		 */
		void seekInterval(long timestamp) {
			// To make sure that the interval of the given timestamp is fully filled,
			// rounds up the seeking timestamp to the smallest timestamp that is
			// a multiple of the interval and is greater than or equal to the given
			// timestamp..
			source.seek(alignTimestamp(timestamp + interval_ms - 1));
			initialized = false;
		}

		/**
		 * Returns the representative timestamp of the current interval.
		 */
		private long getIntervalTimestamp() {
			// NOTE: It is well-known practice taking the start time of
			// a downsample interval as a representative timestamp of it. It also
			// provides the correct context for seek.
			return alignTimestamp(timestamp_end_interval - interval_ms);
		}

		/**
		 * Returns timestamp aligned by interval.
		 */
		private long alignTimestamp(long timestamp) {
			return timestamp - (timestamp % interval_ms);
		}

		// ---------------------- //
		// Doubles interface //
		// ---------------------- //

		@Override
		public boolean hasNextValue() {
			initializeIfNotDone();
			return has_next_value_from_source &&
					next_dp.timestamp() < timestamp_end_interval;
		}

		@Override
		public double nextDoubleValue() {
			if (hasNextValue()) {
				double value = next_dp.toDouble();
				moveToNextValue();
				return value;
			}
			throw new NoSuchElementException("no more values in interval of "
					+ timestamp_end_interval);
		}

		@Override
		public String toString() {
			final StringBuilder buf = new StringBuilder();
			buf.append("ValuesInInterval: ")
					.append("interval_ms=").append(interval_ms)
					.append(", timestamp_end_interval=").append(timestamp_end_interval)
					.append(", has_next_value_from_source=")
					.append(has_next_value_from_source);
			if (has_next_value_from_source) {
				buf.append(", nextValue=(").append(next_dp).append(')');
			}
			buf.append(", source=").append(source);
			return buf.toString();
		}
	}
}
