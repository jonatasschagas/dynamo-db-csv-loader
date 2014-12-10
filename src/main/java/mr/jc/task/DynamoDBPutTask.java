package mr.jc.task;

import java.util.HashMap;
import java.util.Map;

import mr.jc.util.DynamoDBClientFactory;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

public class DynamoDBPutTask implements Runnable 
{
	private Logger logger = Logger.getLogger(DynamoDBPutTask.class);
	private Map<String,String> item;
	private String tableName;
	
	public DynamoDBPutTask(String tableName, Map<String,String> item)
	{
		this.item = item;
		this.tableName = tableName;
	}
	
	@Override
	public void run() 
	{
		try
		{
			AmazonDynamoDBClient dynamoClient = DynamoDBClientFactory.getClient();
			
			Map<String,AttributeValue> dynamoItem = new HashMap<String, AttributeValue>();
			for(String key : item.keySet())
			{
				String value = item.get(key);
				if(value != null && !"".equals(value))
				{
					dynamoItem.put(key,new AttributeValue(value));
				}
			}
			
			PutItemRequest putItemRequest = new PutItemRequest(tableName, dynamoItem);
			dynamoClient.putItem(putItemRequest);
		}
		catch (Exception ex)
		{
			logger.error("run: Error in put request to table: " + tableName + ", record: " + item.toString(),ex);
		}
	}

}
