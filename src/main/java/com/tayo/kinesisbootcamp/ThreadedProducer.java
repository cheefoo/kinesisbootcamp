package com.tayo.kinesisbootcamp;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;


public class ThreadedProducer implements Runnable 
{
	public static final String STREAM_NAME = "<STREAM_NAME>";
	private static final Random RANDOM = new Random();
	private static final Logger logger  = Logger.getLogger(ThreadedProducer.class); 
	private static final String filePath = "C:\\Users\\xxxxx\\workspace\\kinesisbootcamp\\car_odom1.txt";
	private static final String DELIM = ",";
	private List<BootCarObject> carObjectList;
	
	public List<BootCarObject> getCarObjectList() 
	{
		return carObjectList;
	}

	public void setCarObjectList(List<BootCarObject> carObjectList) 
	{
		this.carObjectList = carObjectList;
	}

	public static void asyncPuts (AmazonKinesisAsyncClient kinesis, ThreadedProducer producer) throws InterruptedException, ExecutionException, UnsupportedEncodingException
	{
		//AWSCredentials credentials = new  ProfileCredentialsProvider("default").getCredentials();
		//AmazonKinesisAsyncClient kinesis = new AmazonKinesisAsyncClient(credentials).withRegion(Regions.US_WEST_2);
		//AmazonKinesisClient kinesis = new AmazonKinesisClient(credentials).withRegion(Regions.US_WEST_2);
		
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(STREAM_NAME);
		
		//Todo: Write Handler for DescribeStream
		Future<DescribeStreamResult> result = kinesis.describeStreamAsync(describeStreamRequest);
		//DescribeStreamResult result = kinesis.describeStream(describeStreamRequest);
		
		System.out.println(result.toString());
		
		long createTime = System.currentTimeMillis();
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        
        List<PutRecordsRequestEntry> ptreList = new ArrayList<PutRecordsRequestEntry>();
        int batch = 1;
        
       // long startTime = System.currentTimeMillis();
        List<BootCarObject> carObjects = producer.getCarObjectList();
        while(true)
        {
            for (int i=0; i<carObjects.size(); i++)
            {
            	PutRecordsRequestEntry ptre = new PutRecordsRequestEntry();
            	//Put car object data
            	logger.info("CarObject added is : " + carObjects.get(i).toString());
            	ptre.setData(ByteBuffer.wrap(String.format(carObjects.get(i).toString()).getBytes("UTF-8")));    
            	ptre.setPartitionKey(randomExplicitHashKey());
            	
            	ptreList.add(ptre);
            	long t1 = 0L;
            	if((i%500) == 0)
            	{
            		System.out.println("Batch " + batch + " "+ "Request Size is " + ptreList.size());
            		putRecordsRequest.setRecords(ptreList);
            		putRecordsRequest.setStreamName(STREAM_NAME);
            		Future<PutRecordsResult> putRecordsResult = kinesis.putRecordsAsync(putRecordsRequest, new BootCallbackHandler());
            		logger.info("PutRecordsResult  : " + putRecordsResult.toString() + " has Batch Number : " + batch);
            		t1 = System.currentTimeMillis();		 
            		System.out.println("Batch " + batch + " "+ "Result Size is " + ptreList.size() );
            		logger.info("Batch " + batch + " "+ "Result Size is " + ptreList.size());
            		Thread.sleep(200);
                	batch++;
                	//reinitialize collection
                	ptreList = new ArrayList<PutRecordsRequestEntry>(); 
            	}      	    	 
            }
            carObjects = producer.getCarObjectList(); 
        }
        
        /*long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println("Total Time taken is " + timeTaken);*/
		
	}
	
	 public static String randomExplicitHashKey() 
	    {
	        return new BigInteger(128, RANDOM).toString(10);
	    }

	public void run() 
	{
		AmazonKinesisAsyncClient kinesis = new AmazonKinesisAsyncClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_WEST_2);
		ThreadedProducer producer = new ThreadedProducer();
		List<BootCarObject> carObjects = producer.getDataObjects();
		producer.setCarObjectList(carObjects);
		
		try 
		{
			asyncPuts(kinesis, producer);
		} 
		catch (UnsupportedEncodingException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		} 
		catch (ExecutionException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	public static void main (String [] args) 
	{
		AmazonKinesisAsyncClient kinesis = new AmazonKinesisAsyncClient(new ProfileCredentialsProvider()
    			.getCredentials()).withRegion(Regions.US_WEST_2);
		
		ThreadedProducer producer = new ThreadedProducer();
		List<BootCarObject> carObjects = producer.getDataObjects();
		producer.setCarObjectList(carObjects);
		
		try 
		{
			asyncPuts(kinesis, producer);
		} 
		catch (UnsupportedEncodingException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		} 
		catch (ExecutionException e) 
		{
			e.printStackTrace();
		}
		
	}  
	
	public  List<BootCarObject> getDataObjects()
	{
		List<BootCarObject> carObjectList= new ArrayList<BootCarObject>();
		try 
		{
			FileInputStream fis = new FileInputStream(new File(filePath));
	    	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	    	String line = null;
	    	while ((line = br.readLine()) != null)
	    	{
	        	System.out.println("Line put in stream is " + line);
	        	String [] tokens = line.split(DELIM);
	        	BootCarObject car = new BootCarObject(tokens[0], tokens[1], tokens[2]);
	        	carObjectList.add(car);
	    	}
						
		} 
		catch (FileNotFoundException ex) 
		{
			ex.printStackTrace();
		} 
		catch (IOException ex) 
		{
			ex.printStackTrace();
		} 
		
		return carObjectList;
	}

}
