import org.apache.hadoop.hbase.*;

public final class TrendAnalysis {
	
	private HBaseConfiguration conf;
	private HBaseAdmin admin;
	private HTableDescriptor table;
	private HColumnDescriptor mean;
	private HColumnDescriptor standardDeviation;
	
	public TrendAnalysis() {
		conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		table = new HTableDescriptor("trends");
		mean = new HColumnDescriptor(toBytes("mean"));
		standardDeviation = new HColumnDescriptor(toBytes("standard deviation"));
		table.add(mean);
		table.add(standardDeviation);
	}
	
}