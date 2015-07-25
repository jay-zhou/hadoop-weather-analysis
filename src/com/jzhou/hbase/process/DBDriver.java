package com.jzhou.hbase.process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class DBDriver {

	//private static Log LOG = LoggerFactory.getLogger(HBaseDriver.class);
    private static final Log LOG = LogFactory.getLog(DBDriver.class);
		
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		 LOG.info("Code started");
		
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		 
		 // define scan and define column families to scan
		 Scan scan = new Scan();
		 scan.addFamily(Bytes.toBytes("mp"));

		 Job job = new Job(conf); 
		 
		 job.setJarByClass(DBDriver.class);
		 // define input hbase table
		 TableMapReduceUtil.initTableMapperJob(
		        "weather",
		        scan,
		        DBMapper.class,
		        Text.class,
		        FloatWritable.class,
		        job);
		 // define output table
		 TableMapReduceUtil.initTableReducerJob(
		        "weather_sum",
		        DBReducer.class, 
		        job);

		 job.waitForCompletion(true);

		 LOG.info("Code ended");
		 
	}

}
