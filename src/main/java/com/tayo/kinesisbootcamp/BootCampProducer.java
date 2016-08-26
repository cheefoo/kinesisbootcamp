package com.tayo.kinesisbootcamp;

public class BootCampProducer {

	public static void main(String[] args) 
	{
		Thread single = new Thread(new ThreadedProducer());
		single.start();
		System.out.println("Single put Record Request started");
		//Thread batch = new Thread(new BatchPuts());
		//batch.start();
		//System.out.println("Batch put Records Request started");

	}

}
