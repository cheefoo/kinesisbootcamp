package com.tayo.kinesisbootcamp;

import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class ThreadedConsumer 
{
	private static final Logger logger = Logger.getLogger(ThreadedConsumer.class);
	
	public static void main (String [] args)
	{
		AmazonKinesisClient kinesis = new AmazonKinesisClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_WEST_2);
		
		
		//Get Shard in stream
		//DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		//describeStreamRequest.setStreamName(ThreadedProducer.STREAM_NAME);
		DescribeStreamResult streamResult = kinesis.describeStream(ThreadedProducer.STREAM_NAME);
		StreamDescription streamDescription = streamResult.getStreamDescription();
		List<Shard> shardList = streamDescription.getShards();
		logger.info("Shard size is " + shardList.size());
		
		for (Shard shard: shardList)
		{
			
			String shardIterator = null;
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(ThreadedProducer.STREAM_NAME);
			getShardIteratorRequest.setShardId(shard.getShardId());
			getShardIteratorRequest.setShardIteratorType("LATEST");
			GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
			shardIterator = getShardIteratorResult.getShardIterator();
			
			BootConsumer boot = new BootConsumer(shardIterator, shard.getShardId());
			Thread consumerThread = new Thread(boot);
			logger.info("Starting thread to consumer for Shard :" + shard.getShardId()  + "with ShardIterator :" + shardIterator );
			consumerThread.start();
				
		}	
		
	}
   
}
