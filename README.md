# kinesisbootcamp
This program is written to demonstrate the use of the AWS Kinesis Streams API (NOT the Kinesis Cient Library [KCL]) to produce records into the stream continuosly and read data from the stream continuously. The Stream has 3 shards and it distributes records evenly to all the shards.
#Assumption
Data is not continuous but a FileObject is created in a CarObject to simulate the continuos production of data, the ThreadedProducer Class calls the CarObject to get the fileObject in a loop to simulate the appearance of continuous data.
#Description
To achieve a higher throughput, the ThreadedProducer utilizes the Kinesis Asynchronous Client (AmazonKinesisAsyncClient) to ensure that data is produced to the stream without blocking and a callBack handler is added to get the responses from the requests sent by the AmazonKinesisAsyncClient.
The ThreadedConsumer starts a new thread for each shard to be consumed and each thread does a Kinesis getRecords() call in an infinite loop as prescribed in this documentation below.
http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html
The BootCarRecordsProcessor processes the records received from the BootCOnsumer and also starts multiple threads for each of the batch of objects received from the BootConsumer, it opens a connection to a redshift cluster and persists the records to the database.

#To Use this application
git clone https://github.com/cheefoo/kinesisbootcamp.git
Make the following changes:
1. In the ThreadProducer class, change the filepath variable to your own path.
2. In the ThreadProducer class, change the STREAM_NAME to your own stream and please note the region (I used us-west-2).
3. Update the db.properties to your own database property values

start the consumer 
mvn exec:java -Dexec.mainClass=com.tayo.kinesisbootcamp.ThreadedConsumer (see A below)
start the producer
mvn exec:java -Dexec.mainClass=com.tayo.kinesisbootcamp.ThreadedProducer

For the consumer to make connection the database download (http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver) and install the redshift jar and add it to your path
mvn install:install-file -Dfile=/c/Users/temitayo/workspace/Library/RedshiftJDBC4-1.1.6.1006.jar -DgroupId=com.amazon -DartifactId=redshift.jdbc4 -Dversion=1.1.6.1006 -Dpackaging=jar -DgeneratePom=true
