package com.tayo.kinesisbootcamp;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.model.Record;

public class BootCarRecordsProcessor implements Runnable
{
	private static final Logger logger = Logger.getLogger(BootCarRecordsProcessor.class);
	private final static CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	//private Connection conn;
	private List<Record> recordsList;
	
	
	public BootCarRecordsProcessor(List<Record> recordsList) 
	{
		super();
		this.recordsList = recordsList;
	}

	public List<Record> getRecordsList() 
	{
		return recordsList;
	}

	public void setRecordsList(List<Record> recordsList) 
	{
		this.recordsList = recordsList;
	}
	/*
	 This is my test @Test*/
	public  static void main(String[] args)
	{
		try 
		{
			Connection conn = getConnection();
			logger.info("Connected to DB! smiles..." + conn.toString());
			List<Record> recordList = new ArrayList<Record>();
			
			Record record1 = new Record();
			Record record2 = new Record();
			Record record3 = new Record();
			Record record4 = new Record();
			Record record5 = new Record();
			record1.setData(ByteBuffer.wrap(String.format("Acura NSX 91,1991,254068").getBytes()));
			record2.setData(ByteBuffer.wrap(String.format("Alfa Romeo 147 TI 2.0 TWIN SPARK 06,2006,637568").getBytes()));
			record3.setData(ByteBuffer.wrap(String.format("AEM S2000 (SEMA Gran Turismo Awards 2005),,305500").getBytes()));
			record4.setData(ByteBuffer.wrap(String.format("Acura RSX Type-S 04,2004,279500").getBytes()));
			record5.setData(ByteBuffer.wrap(String.format("Acura NSX RM 91,1991,253500").getBytes()));
			recordList.add(record1);
			recordList.add(record2);
			recordList.add(record3);
			recordList.add(record4);
			recordList.add(record5);
			
			persistRecords(conn, recordList);
			
			
			
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	public void run() 
	{
		try 
		{
			Connection conn = getConnection();
			logger.info("Connected to DB! smiles..." + conn.toString());
			persistRecords(conn, this.getRecordsList());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	
	private static void persistRecords(Connection conn, List<Record> recordsList) throws SQLException 
	{	

		PreparedStatement stmt = null;
		String name = null;
		String year = null;
		int reading = 0;
		logger.info("In persistRecords");
		
		if(recordsList != null && recordsList.size()!=0)
		{
			int k = 1;  // keep track of items added to batch
			String sql = "insert into public.car_odom values (?, ?, ?);";
			stmt = conn.prepareStatement(sql);
			for(Record record: recordsList)
			{	
				try
				{
					String recordValue =  decoder.decode(record.getData()).toString();
					logger.info("RECORD VALUE OBTAINED is : " + recordValue);
					
					String[] rowValues = recordValue.toString().split(",");
					
					if(rowValues.length > 2)
					{
						name = rowValues[0];
						year = rowValues[1];
						reading = Integer.valueOf(rowValues[2]);
						logger.info("name = "+name + ",year= " +year +"reading =" + reading );
						//stmt = conn.prepareStatement(sql);
						stmt.setString(1, name);
						stmt.setString(2, year);
						stmt.setInt(3, Integer.valueOf(reading));
						stmt.addBatch();
						logger.info("Batch " + k + " added successfully" );
						k++;
					}
				} 
				catch (SQLException e) 
				{
					logger.error("Failed adding statement to batch " + e.toString());
				} 
				catch (CharacterCodingException e) 
				{
					logger.error("Record decoding error " + e.toString());
				}		
			}
			try 
			{
				int[] batchExec = stmt.executeBatch();
				
				logger.info("Batch executed successfully for batchSize : " + batchExec.length);
				stmt.close();
			} 
			catch (SQLException e) 
			{
				logger.error("Failed Batch Execution " + e.toString());
			}
			finally 
			{
				try 
				{
					if (stmt != null)
						stmt.close();
				} 
				catch (Exception ex) 
				{
					logger.error("Exception thrown in finally when closing connection " + ex.toString());
				}
			}
		}	
	}
	
	private static Connection getConnection() throws Exception 
	{
		Connection conn = null;
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("db.properties");
        Properties prop = new Properties();
        logger.info("Input from classloader is :" + input.toString());
        prop.load(input);
		try 
		{
			Class.forName("com.amazon.redshift.jdbc4.Driver");
			//Class.forName("org.postgresql.Driver");
			logger.info("Connecting to database...");
			Properties props = new Properties();
			props.setProperty("user", prop.getProperty("dbuser"));
			props.setProperty("password", prop.getProperty("dbpwd"));
			conn = DriverManager.getConnection(prop.getProperty("dburl"), props);			
		} 
		catch (ClassNotFoundException e1) 
		{
			logger.error(e1);
			e1.printStackTrace();
			throw new ClassNotFoundException();
		} 
		catch (SQLException e1) 
		{
			logger.error(e1);
			e1.printStackTrace();
			throw new SQLException();
		}
		
		return conn;
	}

}
