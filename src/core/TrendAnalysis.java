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
	private static final byte[] TRENDS_FAMILY = {'r'};
	private static final byte[] T_FAMILY = {'t'};

	private static byte[] tsdb_table = "tsdb".getBytes();
	private static final byte[] T_QUALIFIER = {'t'};
	
	private Map<String, Long> queue;
	
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
		queue = Collections.synchronizedMap(new LinkedHashMap<String, Long>());

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
						long timeAdded = queue.get(point);
						long currentTime = System.currentTimeMillis() / 1000L;

						// ensures no more data points will be added to this hour
						if(currentTime > timeAdded + 20L) { // added > 2 hours ago -- 20 sec for testing
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

	private void updateTrendData(String dataPoint) {
		try {
			log.info("updating trend data");
			// get info from dataPoint
			String metric = getMetricFromPoint(dataPoint);
			Map<String, String> tags = getTagsFromPoint(dataPoint);
			long timestamp = getTimestampFromPoint(dataPoint);
			short flags = getFlagsFromPoint(dataPoint);

			String row = getTrendsRowKey(metric, tags);
			ArrayList<KeyValue> results = getRowResults(row);
			if(results.size() == 0) {
				initializeNewRows(row, dataPoint);
				results = getRowResults(row);
			}

			byte[] timestampBytes = results.get(0).value();	// get stored timestamp from trends table
			long storedTimestamp = ByteBuffer.wrap(timestampBytes).getLong();
			long pointTimestamp = getTimestampFromPoint(dataPoint); // get timestamp from data point
			
			if (pointTimestamp > storedTimestamp) {
				log.info("comparing stored with point's timestamp");
			    byte[] TSDBRowKey = getTSDBRowKey(metric, tags, pointTimestamp);
				GetRequest getData = new GetRequest(tsdb_table, TSDBRowKey, T_FAMILY);
				
				// look through row backwards to find new data points 
				ArrayList<KeyValue> dataResults = client.get(getData).join();
				log.info("got results = " + dataResults);
				ArrayList<Double> newPoints = new ArrayList<Double>();
				long latestTimestamp = 0L;
				int numResults = dataResults.size();
				for(int i = numResults-1; i >= 0; i--) {
					KeyValue kv = dataResults.get(i);
					long dataPointBaseTime = Bytes.getUnsignedInt(kv.key(),
							tsdb.metrics.width()); // gets base time from row key
					long dataPointTimestamp = Internal.getTimestampFromQualifier(kv.qualifier(),
							dataPointBaseTime);
					dataPointTimestamp = dataPointTimestamp / 1000L; // convert ms to seconds
					log.info("TIMESTAMP = " + dataPointTimestamp);
					if (dataPointTimestamp > storedTimestamp) {
						if (i == numResults-1) {
							latestTimestamp = dataPointTimestamp;
							updateTimeRow(row, latestTimestamp);
						}
						long value = bytesToLong(kv.value());
						newPoints.add((double)value);
					} else {
						break;
					}
				}
				byte[] trendsQualifier = getTrendsQualifier(pointTimestamp);
				String trendsRowKey = getTrendsRowKey(metric, tags);
				updateTrendsRow(trendsRowKey, trendsQualifier, newPoints);
			} else {
				log.info("already updated");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void updateTimeRow(String rowKey, long timestamp) {
		putTimePoint(rowKey + "-count", timestamp);
		putTimePoint(rowKey + "-mean", timestamp);
		putTimePoint(rowKey + "-standard_deviation", timestamp);
	}
	
	/**
	 * Calculate and update trends for row.
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param rowKey
	 * @param qualifier
	 * @param newPoints
	 */
	private void updateTrendsRow(String rowKey,
			byte[] qualifier, ArrayList<Double> values) {
		try {
			// get old trends
			double oldCount = getTrendsPoint(rowKey + "-count", qualifier);
			double oldMean = getTrendsPoint(rowKey + "-mean", qualifier);
			double oldStdev = getTrendsPoint(rowKey + "-standard_deviation", qualifier);
		
			double newCount = values.size();
			double sum = 0;
			for(double value : values) {
				sum += value;
			}
			double newMean = sum / newCount;
			double diffSqSum = 0;
			for(double value : values) {
				diffSqSum += diffSqSum = Math.pow(value - newMean, 2);
			}
			double newStdev = diffSqSum / (newCount - 1);
			double[] results = {newCount, newMean, newStdev};
			
			// update old trends based on new points
			double updatedCount = oldCount + newCount;
			double updatedMean = (oldCount * oldMean + newCount * newMean)
					/ (oldCount + newCount);
			double updatedStdev = Math.sqrt(
					(oldCount * Math.pow(oldStdev, 2) + newCount * Math.pow(newStdev, 2))
					/ (oldCount + newCount)
					+ ((oldCount * newCount) / (oldCount + newCount))
					* Math.pow(oldStdev - newStdev, 2));
			
			// puts updated trends into HBase
			putTrendsPoint(rowKey + "-count", qualifier, updatedCount);
			putTrendsPoint(rowKey + "-mean", qualifier, updatedMean);
			putTrendsPoint(rowKey + "-standard_deviation", qualifier, updatedStdev);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private ArrayList<KeyValue> getRowResults(String row) {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			String rowCount = row + "-count"; // get count (if count exists, mean and stdev should too)
			final byte[] rowCountBytes = rowCount.getBytes();
			GetRequest getTimestamp = new GetRequest(trends_table,
					rowCountBytes, T_FAMILY, T_QUALIFIER);
			results = client.get(getTimestamp).join();
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
		log.info("initializing new rows");

		long timestamp = getTimestampFromPoint(point);
		byte[] qualifier = getTrendsQualifier(timestamp);

		putTrendsPoint(row + "-count", qualifier, 1);
		putTrendsPoint(row + "-mean", qualifier, getValueFromPoint(point));
		putTrendsPoint(row + "-standard_deviation", qualifier, 0);
		
		putTimePoint(row + "-count", 0);
		putTimePoint(row + "-mean", 0);
		putTimePoint(row + "-standard_deviation", 0);
		client.flush();
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
		String dataPoint = buildPointString(metric, value, timestamp, tags, flags);
		long currentTime = System.currentTimeMillis() / 1000L;

	    if(queue.containsKey(dataPoint)) {
	    	log.info("removing " + dataPoint + " from queue");
	    	queue.remove(dataPoint); // remove and insert to update order
	    }
	    log.info("adding " + dataPoint + " to queue");
    	queue.put(dataPoint, currentTime);
	}
	
	public void shutdown(){
		client.flush();
		client.shutdown();
	}
	
	/*======== GENERAL HELPER METHODS ============ */
	/**
	 * Builds the row key in the trends table.
	 * @param metric
	 * @param tags
	 * @return trends table row key
	 */
	private String getTrendsRowKey(String metric, Map<String, String> tags) {
		ArrayList<String> tagsList = new ArrayList<String>(tags.keySet());
		Collections.sort(tagsList);
		log.info("size of tagsList is======" + tagsList.size());
		String tagsAndValues = "";
		for(String tag : tagsList) {
			tagsAndValues = tagsAndValues + "-" + tag + "=" + tags.get(tag);
		}
		log.info("trends row key = " + metric + tagsAndValues);
		return metric + tagsAndValues;
	}
	
	/**
	 * Gets the row key of the TSDB table that contains the original data.
	 * @param metric
	 * @param tags
	 * @param timestamp
	 * @return TSDB table row key
	 */
	private byte[] getTSDBRowKey(String metric, Map<String, String> tags, long timestamp) {
		final byte[] rowKey = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
	    final long base_time;
	    if ((timestamp & Const.SECOND_MASK) != 0) {
	        base_time = ((timestamp / 1000) - 
	            ((timestamp / 1000) % Const.MAX_TIMESPAN));
	      } else {
	        base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
	      }
	    Bytes.setInt(rowKey, (int) base_time, tsdb.metrics.width());
	    return rowKey;
	}
	
	/**
	 * Given the timestamp, builds and returns the trends table qualifier
	 * in the format of Day-Hour where Sunday = 1, Monday = 2, etc.
	 * and 10:04:15.250 PM = 22.
	 * @param timestamp
	 * @return trends table qualifier
	 */
	private byte[] getTrendsQualifier(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		String qualifier = cal.get(Calendar.DAY_OF_WEEK)
				+ "-" + cal.get(Calendar.HOUR_OF_DAY);
		return qualifier.getBytes();
	}
	
	private double getTrendsPoint(String rowKey, byte[] qualifier) {
		byte[] row = rowKey.getBytes();
		GetRequest request = new GetRequest(trends_table, row, TRENDS_FAMILY, qualifier);
		byte[] bytes = client.get(request).join().get(0).value();
		double value = ByteBuffer.wrap(bytes).getDouble();
		return value;
	}
	
	private void putTrendsPoint(String rowKey, byte[] qualifier, double value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		byte[] row = rowKey.getBytes();
		PutRequest put = new PutRequest(trends_table, row, TRENDS_FAMILY, qualifier, bytes);
		client.put(put);
	}
	
	private void putTimePoint(String rowKey, long timestamp) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putLong(timestamp);
		byte[] row = rowKey.getBytes();
		PutRequest put = new PutRequest(trends_table, row, T_FAMILY, T_QUALIFIER, bytes);
		client.put(put);
	}
	
	/**
	 * Given a byte array, converts it to a long.
	 * @param bytes Byte array to convert
	 * @return long 
	 */
	private long bytesToLong(byte[] bytes) {
		long longValue = 0;
		for (int i = 0; i < bytes.length; i++) {
			longValue += ((long) bytes[i] & 0xffL) << (8 * i);
		}
		return longValue;
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
	private String buildPointString(String metric, byte[] value,
			long timestamp, Map<String, String> tags, short flags) {
		long v = bytesToLong(value);
		return getTrendsRowKey(metric, tags) + "-" + String.valueOf(flags)
			+ "-" + String.valueOf(timestamp) + "-" + v;
	}
	
	private String getMetricFromPoint(String point) {
		return point.split("-")[0];
	}
	
	private Map<String, String> getTagsFromPoint(String point) {
		Map<String, String> tags = new HashMap<String, String>();
		String[] rowData = point.split("-");
		for(int i = 1; i < rowData.length-3; i++) {
			String[] tagPair = rowData[i].split("=");
			tags.put(tagPair[0], tagPair[1]);
		}
		return tags;
	}
	
	private short getFlagsFromPoint(String point) {
		String[] rowData = point.split("-");
		String flag = rowData[rowData.length-3];
		return Short.parseShort(flag);
	}
	
	private long getTimestampFromPoint(String point) {
		String[] rowData = point.split("-");
		String timestamp = rowData[rowData.length-2];
		return Long.parseLong(timestamp);
	}
	
	private long getValueFromPoint(String point) {
		String[] rowData = point.split("-");
		String value = rowData[rowData.length-1];
		return Long.parseLong(value);
	}
}
