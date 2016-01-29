package net.opentsdb.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.nio.ByteBuffer;

import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;

public class TrendAnalysis {
	
	private static HBaseClient client;
	private final Config config;
	private TSDB tsdb;
	
	private static byte[] trends_table = "trends".getBytes();
	private static final byte[] TRENDS_FAMILY = {'r'}; // stores trends
	private static final byte[] T_FAMILY = {'t'}; // stores latest timestamp

	private static byte[] tsdb_table = "tsdb".getBytes();
	private static final byte[] T_QUALIFIER = {'t'};
	
	private Map<String, String> queue;
	
	static Logger log = LoggerFactory.getLogger(TrendAnalysis.class);

	/**
	 * Creates a trends table in HBase that stores the mean
	 * and standard deviation for each hour of each day of
	 * @param client
	 * @param config
	 */
	public TrendAnalysis(HBaseClient client, final Config config, TSDB tsdb){
		this.client = client;
		this.config = config;
		this.tsdb = tsdb;
		queue = Collections.synchronizedMap(new LinkedHashMap<String, String>());
		startThread();
	}
	
	/**
	 * Alternate constructor
	 * @param config An initialized configuration object
	 */
	public TrendAnalysis(final Config config, TSDB tsdb) {
	    this(new HBaseClient(config.getString("tsd.storage.hbase.zk_quorum"),
	                         config.getString("tsd.storage.hbase.zk_basedir")),
	    		config, tsdb);
	  }
	
	/**
	 * Thread that checks the queue every 2 hours. 
	 * If there are rows that has been in the queue for at least 2
	 * hours, then update the trend for that row if it hasn't already
	 * been updated.
	 */
	private void startThread() {
		Thread thread = new Thread(new Runnable() {
			public void run() {
				while(true) {
					try{
						log.info("start sleep");
						Thread.sleep(20000); // Sleeps for 2 hours -- 20 sec for testing
						log.info("done sleeping");
					} catch (Exception e) {
						e.printStackTrace();
					}
					log.info("start looking through the queue");
					Iterator<String> iterator = queue.keySet().iterator();
					while(iterator.hasNext()) {
						String point = iterator.next();
						log.info("looking at row : " + point);
						String[] point_data= queue.get(point).split("-");
						long time_added = Long.parseLong(point_data[0]);
						long current_time = System.currentTimeMillis() / 1000L;

						// ensures no more data points will be added to this hour
						if(current_time > time_added + 20L) { // added > 2 hours ago -- 20 sec for testing
							log.info("row added > 10 sec ago");
							updateTrendData(point);
							iterator.remove();
						} else {
							// points are sorted by time added so
							// all points after this will be even more recent
							break;
						}
					}
				}
			}
		});
		thread.start();
	}

	/**
	 * Update the trends for the row if it hasn't 
	 * already been updated by another machine.
	 * @param point
	 */
	private void updateTrendData(String point) {
		try {
			log.info("updating trend data");
			// get info from dataPoint
			String metric = getMetricFromPoint(point);
			Map<String, String> tags = getTagsFromPoint(point);
			long point_timestamp = getTimestampFromPoint(point);

			String row = getTrendsRowKey(metric, tags);
			ArrayList<KeyValue> results = getRowResults(row);
			byte[] qualifier = getTrendsQualifier(point_timestamp);
			if(!rowColumnExists(row, qualifier)) {
				initializeNewRows(row, point);
			} else {
				// wait until initialized rows has been flushed
				while(results.size() == 0) {
					results = getRowResults(row);
				}
				
				long stored_timestamp = ByteBuffer.wrap(results.get(0).value()).getLong();
				
				log.info("point_timestamp = " + point_timestamp);
				log.info("stored_timestamp = " + stored_timestamp);
				if (point_timestamp > stored_timestamp) {
					log.info("comparing stored with point's timestamp");
				  byte[] tsdb_row_key = getTSDBRowKey(metric, tags, point_timestamp);
				  log.info("tsdb_row_key = " + String.valueOf(tsdb_row_key));
					GetRequest getData = new GetRequest(tsdb_table, tsdb_row_key, T_FAMILY);
					
					// look through row backwards to find new data points
					final Query query = tsdb.newQuery();
					query.setStartTime(stored_timestamp + 1L);
					query.setTimeSeries(metric, tags, Aggregators.SUM, false);
					DataPoints[] data_points = (DataPoints[]) query.runAsync().join();
					
					for(DataPoints dps : data_points){
						int size = dps.aggregatedSize();
						log.info("got results = " + dps);
						for(int i = size-1; i >= 0; i--) {
							ArrayList<Double> new_points = new ArrayList<Double>();
							long latest_timestamp = 0L;
							long row_point_timestamp = dps.timestamp(i);
							row_point_timestamp /= 1000L; // convert from ms to sec
							log.info("row_point_timestamp = " + row_point_timestamp);
							log.info("stored_timestamp = " + stored_timestamp);
							if (row_point_timestamp > stored_timestamp) {
								log.info("updating!!!!!!");
								if (i == size-1) { // updated stored timestamp with latest timestamp
									latest_timestamp = row_point_timestamp;
									updateTimeRow(row, latest_timestamp);
									log.info("updated latest stored time to " + row_point_timestamp);
								}
								long value = dps.longValue(i);
								new_points.add((double)value);
								log.info("added to queue" + (double) value);
								String row_key = getTrendsRowKey(metric, tags);
								updateTrendsRow(row_key, qualifier, new_points);
							} else {
								break;
							}
							
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Update the stored timestamp for each trend of the given row.
	 * @param row_key
	 * @param timestamp
	 */
	private void updateTimeRow(String row_key, long timestamp) {
		putTimePoint(row_key + "-count", timestamp);
		putTimePoint(row_key + "-mean", timestamp);
		putTimePoint(row_key + "-standard_deviation", timestamp);
	}
	
	/**
	 * Calculate and update trends for row.
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param rowKey
	 * @param qualifier
	 * @param newPoints
	 */
	private void updateTrendsRow(String row_key,
			byte[] qualifier, ArrayList<Double> values) {
		try {
			// get old trends
			log.info("updatedTrendsRow qualifier = " + String.valueOf(qualifier));
			double old_count = getTrendsPoint(row_key + "-count", qualifier);
			double old_mean = getTrendsPoint(row_key + "-mean", qualifier);
			double old_stdev = getTrendsPoint(row_key + "-standard_deviation", qualifier);
		  
			double new_count = values.size();
			double sum = 0;
			for(double value : values) {
				sum += value;
			}
			double new_mean = sum / new_count;
			double diffSqSum = 0;
			for(double value : values) {
				diffSqSum += Math.pow(value - new_mean, 2);
			}
			double new_stdev = diffSqSum / new_count;
			double[] results = {new_count, new_mean, new_stdev};
			
			// update old trends based on new points
			double updated_count = old_count + new_count;
			double updated_mean = (old_count * old_mean + new_count * new_mean)
					/ (old_count + new_count);
			double updated_stdev = Math.sqrt(
					(old_count * Math.pow(old_stdev, 2) + new_count * Math.pow(new_stdev, 2))
					/ (old_count + new_count)
					+ ((old_count * new_count) / Math.pow((old_count + new_count), 2))
					* Math.pow(old_mean - new_mean, 2));

			// puts updated trends into HBase
			putTrendsPoint(row_key + "-count", qualifier, updated_count);
			putTrendsPoint(row_key + "-mean", qualifier, updated_mean);
			putTrendsPoint(row_key + "-standard_deviation", qualifier, updated_stdev);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private boolean rowColumnExists(String row, byte[] qualifier) {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			String row_count = row + "-count"; // get count (if count exists, mean and stdev should too)
			final byte[] row_count_bytes = row_count.getBytes();
			GetRequest request = new GetRequest(trends_table,
					row_count_bytes, TRENDS_FAMILY, qualifier);
			results = client.get(request).join();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results.size() != 0;
	}
	
	private ArrayList<KeyValue> getRowResults(String row) {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			String row_count = row + "-count"; // get count (if count exists, mean and stdev should too)
			final byte[] row_count_bytes = row_count.getBytes();
			GetRequest request = new GetRequest(trends_table,
					row_count_bytes, T_FAMILY, T_QUALIFIER);
			results = client.get(request).join();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * initialize 3 rows (count, mean, stdev) in trends table
	 * for trends family and timestamp for each row.
	 * @param row
	 * @param point
	 */
	private void initializeNewRows(String row, String point) {
		long timestamp = getTimestampFromPoint(point);
		byte[] qualifier = getTrendsQualifier(timestamp);
		String[] point_data = queue.get(point).split("-");
		long value = Long.parseLong(point_data[1]);
		
		log.info("initializing this row: " + row + "-count");
		putTrendsPoint(row + "-count", qualifier, 1);
		putTrendsPoint(row + "-mean", qualifier, (double) value);
		putTrendsPoint(row + "-standard_deviation", qualifier, 0);
		
		putTimePoint(row + "-count", timestamp);
		putTimePoint(row + "-mean", timestamp);
		putTimePoint(row + "-standard_deviation", timestamp);
		
		log.info("NEW ROWS ADDED");
	}

	/**
	 * Adds a new point. If it is already in the queue, update
	 * the timestamp of when it was added and move to end of queue.
	 * If not already in the queue, then add it with current timestamp.
	 * @param metric
	 * @param timestamp
	 * @param value
	 * @param tags
	 */
	public void addPoint(String metric, byte[] value,
			long timestamp, Map<String, String> tags, short flags) {
		log.info("trendAnalysis adding point!!!!!!!!!!!!!! !!!!!!!!");
		
		// add new data point to queue
		String point = buildPointString(metric, timestamp, tags);
		long current_time = System.currentTimeMillis() / 1000L;

	    if(queue.containsKey(point)) {
	    	log.info("removing " + point + " from queue");
	    	queue.remove(point); // remove and insert to update order
	    }
	    log.info("adding " + point + " to queue");
    	queue.put(point, current_time + "-" + bytesToLong(value));
	}
	
	public void shutdown(){
		client.flush();
		client.shutdown();
	}
	
	/*======== METHODS TO GET TRENDS ============ */
	/**
	 * Given the metric and tags, returns the requsted trend for 
	 * the specified day of the week and hour of the day.
	 * @param metric
	 * @param tags
	 * @param day
	 * @param hour
	 * @param trend_name
	 * @return trend information
	 */
	public static double getTrendForTimestamp(String metric, Map<String, String> tags,
			long timestamp, String trend_name) {
		String row_key = getTrendsRowKey(metric, tags);
		if(trend_name == "count" || trend_name == "mean" || trend_name == "standard_deviation") {
			row_key += "-" + trend_name;
			log.info("row key = " + row_key);
			final byte[] row = row_key.getBytes();
			log.info("timestamp = " + timestamp);
			final byte[] qualifier = getTrendsQualifier(timestamp / 1000L);
			log.info("getting: row = " + row + " family = " + String.valueOf(TRENDS_FAMILY) + " qualifier = " + String.valueOf(qualifier));
			GetRequest request = new GetRequest(trends_table, row, TRENDS_FAMILY, qualifier);
			try {
				long startTime = System.currentTimeMillis(); //fetch starting time
				ArrayList<KeyValue> result = client.get(request).join();
				double trend = 0;
				if(result.size() > 0) {
					byte[] result_value = result.get(0).value();
					trend = ByteBuffer.wrap(result_value).getDouble();
					log.info("RETURNING TREND = " + trend);
				} else {
					throw new Exception();
				} 
				return trend;
			} catch (Exception e) {
				log.info("ERROR: cannot get trends");
				return 0;
			}
		} else {
			log.info("ERROR: " + trend_name + " is not a valid trend");
			return 0;
		}
	}
	
	/*======== GENERAL HELPER METHODS ============ */
	/**
	 * Builds the row key in the trends table.
	 * @param metric
	 * @param tags
	 * @return trends table row key
	 */
	private static String getTrendsRowKey(String metric, Map<String, String> tags) {
		ArrayList<String> tags_list = new ArrayList<String>(tags.keySet());
		Collections.sort(tags_list);
		String tags_and_values = "";
		for(String tag : tags_list) {
			tags_and_values += "-" + tag + "=" + tags.get(tag);
		}
		return metric + tags_and_values;
	}
	
	/**
	 * Gets the row key of the TSDB table that contains the original data.
	 * @param metric
	 * @param tags
	 * @param timestamp
	 * @return TSDB table row key
	 */
	private byte[] getTSDBRowKey(String metric, Map<String, String> tags, long timestamp) {
		final byte[] row_key = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
	    final long base_time;
	    if ((timestamp & Const.SECOND_MASK) != 0) {
	        base_time = ((timestamp / 1000) - 
	            ((timestamp / 1000) % Const.MAX_TIMESPAN));
	      } else {
	        base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
	      }
	    Bytes.setInt(row_key, (int) base_time, tsdb.metrics.width());
	    return row_key;
	}
	
	/**
	 * Given the timestamp, builds and returns the trends table qualifier
	 * in the format of Day-Hour where Sunday = 1, Monday = 2, etc.
	 * and 10:04:15.250 PM = 22.
	 * @param timestamp
	 * @return trends table qualifier
	 */
	private static byte[] getTrendsQualifier(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		String qualifier = cal.get(Calendar.DAY_OF_WEEK)
				+ "-" + cal.get(Calendar.HOUR_OF_DAY);
		log.info("qualifier = " + qualifier);
		return qualifier.getBytes();
	}
	
	private double getTrendsPoint(String row_key, byte[] qualifier) {
		double value = 0;
		try {
			byte[] row = row_key.getBytes();
			GetRequest request = new GetRequest(trends_table, row, TRENDS_FAMILY, qualifier);
			byte[] bytes = client.get(request).join().get(0).value();
			value = ByteBuffer.wrap(bytes).getDouble();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
	
	private void putTrendsPoint(String row_key, byte[] qualifier, double value) {
		log.info("updating " + row_key + " value to " + value + "###################");
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		byte[] row = row_key.getBytes();
		PutRequest put = new PutRequest(trends_table, row, TRENDS_FAMILY, qualifier, bytes);
		client.put(put);
		client.flush();
		log.info("flushed " + row_key);
	}
	
	private void putTimePoint(String row_key, long timestamp) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putLong(timestamp);
		byte[] row = row_key.getBytes();
		PutRequest put = new PutRequest(trends_table, row, T_FAMILY, T_QUALIFIER, bytes);
		client.put(put);
		client.flush();
		log.info("flushed " + row_key);
	}
	
	/**
	 * Given a byte array, converts it to a long.
	 * @param bytes Byte array to convert
	 * @return long 
	 */
	private long bytesToLong(byte[] bytes) {
		long long_value = 0;
		for (int i = 0; i < bytes.length; i++) {
			long_value = (long_value << 8) + (bytes[i] & 0xff);
		}
		return long_value;
	}
	
	/*======== METHODS TO PARSE OR BUILD DATA FOR POINT ============ */
	/**
	 * Given the information about the point, build the String
	 * that represents the point.
	 * @param metric
	 * @param value
	 * @param timestamp
	 * @param tags
	 * @param flags
	 * @return a String representation of the data point.
	 */
	private String buildPointString(String metric,
			long timestamp, Map<String, String> tags) {
		return getTrendsRowKey(metric, tags) + "-" + String.valueOf(timestamp);
	}
	
	private String getMetricFromPoint(String point) {
		return point.split("-")[0];
	}
	
	private Map<String, String> getTagsFromPoint(String point) {
		Map<String, String> tags = new HashMap<String, String>();
		String[] data = point.split("-");
		for(int i = 1; i < data.length-1; i++) {
			String[] tagPair = data[i].split("=");
			tags.put(tagPair[0], tagPair[1]);
		}
		return tags;
	}
	
	private long getTimestampFromPoint(String point) {
		String[] data = point.split("-");
		String timestamp = data[data.length-1];
		return Long.parseLong(timestamp);
	}

}
