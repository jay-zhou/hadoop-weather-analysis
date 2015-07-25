package com.jzhou.hbase.process;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class DBReducer extends TableReducer<Text, FloatWritable, ImmutableBytesWritable>{
	 
	 //@Override
	 public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
	   throws IOException, InterruptedException {
		 
	     
		 try {
			 
	        Float sum = 0.0f;
	        // loop through different countries and add it to sum
	        for (FloatWritable val : values) {	
	        
	            sum += (float)val.get();
	        } 
	   
	        // calculate 10 years data
	        sum = sum * 10;
	        
	        // create hbase put 
	        Put insHBase = new Put(Bytes.toBytes(key.toString()));
	        
	        // insert sum value to hbase 
	        insHBase.add(Bytes.toBytes("mp"), Bytes.toBytes("sum"), Bytes.toBytes(sum));
	        // write data to Hbase table
	        context.write(null, insHBase);

	     } catch (Exception e) {
	     e.printStackTrace();
	  }
   }

}
