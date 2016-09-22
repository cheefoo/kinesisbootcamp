package com.tayo.kinesisbootcamp;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;


public class BootCallbackHandler implements AsyncHandler<PutRecordsRequest, PutRecordsResult> 
{
	private static final Logger logger  = Logger.getLogger(BootCallbackHandler.class); 

	public void onError(Exception exception) 
	{
		// TODO Auto-generated method stub
		
	}

	public void onSuccess(PutRecordsRequest request, PutRecordsResult result)
    {
        final List<PutRecordsResultEntry> putRecordsResultEntryList = result.getRecords();
        final List<PutRecordsRequestEntry> failedRecordList = new ArrayList<PutRecordsRequestEntry>();
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = request.getRecords();

        System.out.println("Failed Count is :" + result.getFailedRecordCount());
        logger.info("Failed Count is :" + result.getFailedRecordCount());

        for (int i = 0; i < putRecordsResultEntryList.size(); i++)
        {
            final PutRecordsResultEntry resultEntry = putRecordsResultEntryList.get(i);
            final PutRecordsRequestEntry requestEntry = putRecordsRequestEntryList.get(i);
            String errorCode = resultEntry.getErrorCode();
            if (errorCode != null || errorCode.length() != 0)
            {
                System.out.println("Failed Record is:" + resultEntry.toString());
                logger.info("Failed Record is:" + resultEntry.toString());
                failedRecordList.add(requestEntry);
            }
        }
    }
        
	
}
