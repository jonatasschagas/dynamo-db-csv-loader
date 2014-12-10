package mr.jc.util;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 * 
 * Reads the credentials from ~/.aws/config file
 * 
 * @author jonataschagas
 *
 */
public class DynamoDBClientFactory 
{

	private static final Logger logger = Logger.getLogger(DynamoDBClientFactory.class);
	private static AmazonDynamoDBClient client;
	private static AmazonDynamoDBAsyncClient asyncClient;

	public synchronized static AmazonDynamoDBClient getClient() {
		if(client == null) {
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			String key = credentials.getAWSAccessKeyId();
			logger.info("Using AWS key: " + key);
			client = new AmazonDynamoDBClient(credentials);
		}
		return client;
	}

	public synchronized static AmazonDynamoDBAsyncClient getAsyncClient() {
		if(asyncClient == null) {
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			String key = credentials.getAWSAccessKeyId();
			logger.info("Using AWS key: " + key);
			asyncClient = new AmazonDynamoDBAsyncClient(credentials);
		}
		return asyncClient;
	}

}
