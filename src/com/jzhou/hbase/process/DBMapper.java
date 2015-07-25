package com.jzhou.hbase.process;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class DBMapper extends TableMapper<Text, FloatWritable> {
	
	 
	private Text outputKey = new Text();
	
	//@Override
	public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
	   throws IOException, InterruptedException {
		 

	     try {
	   	   
	         // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
	         byte[] bJan = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("jan"));
	         String sJan = new String(bJan);
	         Float fJan = Float.parseFloat(sJan);
	         outputKey.set("jan");
	         // emit date and sales values
	         context.write(outputKey, new FloatWritable(fJan));
	
	        
	         byte[] bFeb = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("feb"));
	    	 String sFeb = new String(bFeb);
	    	 Float fFeb = Float.parseFloat(sFeb);
	    	 outputKey.set("feb");
	    	 // emit date and sales values
	         context.write(outputKey, new FloatWritable(fFeb));
	        
	         
	    	 byte[] bMar = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("mar"));
	    	 String sMar = new String(bMar);
	    	 Float fMar = Float.parseFloat(sMar);
	    	 outputKey.set("mar");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fMar));
	    	   
	    	 byte[] bApr = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("apr"));
	    	 String sApr = new String(bApr);
	    	 Float fApr = Float.parseFloat(sApr);
	    	 outputKey.set("apr");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fApr));
	    	   
	    	 byte[] bMay = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("may"));
	    	 String sMay = new String(bMay);
	    	 Float fMay = Float.parseFloat(sMay);
	    	 outputKey.set("may");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fMay));
	    	   
	    	 byte[] bJun = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("jun"));
	    	 String sJun = new String(bJun);
	    	 Float fJun = Float.parseFloat(sJun);
	    	 outputKey.set("jun");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fJun));
	    	   
	    	 byte[] bJul = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("jul"));
	    	 String sJul = new String(bJul);
	    	 Float fJul = Float.parseFloat(sJul);
	    	 outputKey.set("jul");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fJul));
	    	   
	    	 byte[] bAug = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("aug"));
	    	 String sAug = new String(bAug);
	    	 Float fAug = Float.parseFloat(sAug);
	    	 outputKey.set("aug");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fAug));
	    	   
	    	 byte[] bSep = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("sep"));
	    	 String sSep = new String(bSep);
	    	 Float fSep = Float.parseFloat(sSep);
	    	 outputKey.set("sep");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fSep));
	    	   
	    	 byte[] bOct = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("oct"));
	    	 String sOct = new String(bOct);
	    	 Float fOct = Float.parseFloat(sOct);
	    	 outputKey.set("oct");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fOct));
	    	   
	    	 byte[] bNov = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("nov"));
	    	 String sNov = new String(bNov);
	    	 Float fNov = Float.parseFloat(sNov);
	    	 outputKey.set("nov");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fNov));
	    	   
	    	 byte[] bDec = columns.getValue(Bytes.toBytes("mp"), Bytes.toBytes("dec"));
	    	 String sDec = new String(bDec);
	    	 Float fDec = Float.parseFloat(sDec);
	    	 outputKey.set("dec");
	    	 // emit date and sales values
	    	 context.write(outputKey, new FloatWritable(fDec));   
	   
	  
	         
	     } catch (RuntimeException e){
	     e.printStackTrace();
	  }
    }
}
