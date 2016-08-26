package com.tayo.kinesisbootcamp;

public class BootCarObject 
{
	private String name;
	private String year;
	private String odometer;
	public String getName() 
	{
		return name;
	}
	public void setName(String name) 
	{
		this.name = name;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) 
	{
		this.year = year;
	}
	public String getOdometer() 
	{
		return odometer;
	}
	public void setOdometer(String odometer) 
	{
		this.odometer = odometer;
	}
	public BootCarObject(String name, String year, String odometer) 
	{
		super();
		this.name = name;
		this.year = year;
		this.odometer = odometer;
	}
	@Override
	public String toString() 
	{
		return  name + "," + year + "," + odometer;
	}
	
	

}
