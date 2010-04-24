// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.Map;

import net.opentsdb.HBaseException;

/**
 * Represents a mutable sequence of continuous data points.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public interface WritableDataPoints extends DataPoints {

  /**
   * Sets the metric name and tags of the series.
   * <p>
   * This method can be called multiple times on the same instance to start
   * adding data points to another time series without having to create a new
   * instance.  This particularly helps when using {@link #groupCommit}.
   * @param metric A non-empty string.
   * @param tags The tags on this series.  This non-empty list must have an
   * even number of elements.  The elements in the list must be alternating
   * between a tag name and a tag value, starting with a tag name.  Example:
   * <code>{ "host", "web42", "iface", "eth2" }</code>
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   */
  void setSeries(String metric, Map<String, String> tags);

  /**
   * Adds a {@code long} data point to the TSDB.
   * <p>
   * The data point is immediately persisted unless {@link #groupCommit} is
   * used.  Data points must be added in chronological order.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws HBaseException if there was a problem while persisting data.
   */
  void addPoint(long timestamp, long value);

  /**
   * Appends a {@code float} data point to this sequence.
   * <p>
   * The data point is immediately persisted unless {@link #groupCommit} is
   * used.  Data points must be added in chronological order.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the value is {@code NaN} or
   * {@code Infinite}.
   * @throws HBaseException if there was a problem while persisting data.
   */
  void addPoint(long timestamp, float value);

  /**
   * Changes the size of the group commits.
   * <p>
   * By calling this method, one can set the <i>approximate</i> size of the
   * group commits.  {@code 0} (the default) means data points are persisted
   * immediately.
   * <p>
   * Calling this method with a lower value than what was previously set may
   * cause it to persist uncommitted data points.
   * @param size The approximate number of data points that should be
   * accumulated client-side before being sent to HBase.  The actual size of
   * the commits is defined in bytes not in data points, so the number of data
   * points grouped per commit will vary slightly depending on the size of
   * each data point.
   * @throws IllegalArgumentException if the argument is negative or zero.
   * @throws HBaseException if there was a problem while persisting data.
   */
  void groupCommit(short size);

  /**
   * Specifies whether or not this is a batch import.
   * <p>
   * It is preferred that this method be called for anything importing a batch
   * of data points (as opposed to streaming in new data points in real time).
   * <p>
   * Calling this method changes a few important things:
   * <ul>
   * <li>Data points may not be persisted immediately.  In the event of an
   * outage in HBase during or slightly after the import, un-persisted data
   * points will be lost.</li>
   * <li>{@link #groupCommit groupCommit} may be called with an argument
   * chosen by the implementation.</li>
   * </ul>
   * @param batchornot if true, then this is a batch import.
   */
  void setBatchImport(boolean batchornot);

}