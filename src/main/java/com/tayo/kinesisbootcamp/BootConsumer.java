package com.tayo.kinesisbootcamp;

import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

public class BootConsumer implements Runnable
{
	private static final Logger logger = Logger.getLogger(BootConsumer.class);
	private String shardIterator;
	private String shardId;
	private AmazonKinesisClient kinesis = new AmazonKinesisClient(new ProfileCredentialsProvider()
			.getCredentials()).withRegion(Regions.US_WEST_2);
	
	public String getShardIterator() 
	{
		return shardIterator;
	}

	public void setShardIterator(String shardIterator) 
	{
		this.shardIterator = shardIterator;
	}

	public BootConsumer(String shardIterator) 
	{
		super();
		this.shardIterator = shardIterator;
	}
	
	public BootConsumer(String shardIterator, String shardId) 
	{
		super();
		this.shardIterator = shardIterator;
		this.shardId = shardId;
	}

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public void run() 
	{
		List<Record> records;
		while (true) 
		{		
		  GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		 // logger.info("Current ShardIterator is : " + this.getShardIterator() + " Current ShardId is : " + this.getShardId());
		  getRecordsRequest.setShardIterator(this.getShardIterator());
		  getRecordsRequest.setLimit(250); 

		  GetRecordsResult result = kinesis.getRecords(getRecordsRequest);
		  
		  records = result.getRecords();
		  logger.info("Records Size is : " + records.size() + " Records : " + records.toString());
		  
		  try 
		  {
			  Thread.sleep(1000);
		  } 
		  catch (InterruptedException exception) 
		  {
		    throw new RuntimeException(exception);
		  }
		  if(records.size() > 1)
		  {
			  //spin up new thread and offload processing to thread
			  BootCarRecordsProcessor bcrProcessor = new BootCarRecordsProcessor(records);
			  Thread bcrThread = new Thread(bcrProcessor);
			  logger.info("Starting processor thread for " + bcrThread.getName());
			  bcrThread.start();
		  }
		  
		  shardIterator = result.getNextShardIterator();
		}
		
	}

}
