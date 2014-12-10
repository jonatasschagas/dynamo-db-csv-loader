package mr.jc.run;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import mr.jc.task.DynamoDBPutTask;
import mr.jc.util.MyThreadFactory;
import mr.jc.util.NotifyingBlockingThreadPoolExecutor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

/**
 * DynamoDB Loader application.
 * 
 * usage: java -jar dynamo-db-loader.jar 
 * 
 * @author jonataschagas
 *
 */
public class Application 
{
	
	private static final int KEEP_ALIVE_TIME = 60;
	private static final long BLOCKING_TIMEOUT = 60l;
	private static final String DYNAMO_DB_LOADER_POOL ="dynamo-loader-pool"; 
	
	private static Logger logger = Logger.getLogger(Application.class);
	private static ThreadPoolExecutor poolExecutor;
	
	public static void main(String args[]) throws IOException
	{
		Namespace ns = parseArgs(args);
		
		String table = ns.getString("table");
		String header = ns.getString("header");
		Integer numberOfThreads = ns.getInt("threads");
		List<String> fileNames = ns.<String> getList("file");
		
		logger.info("dynamo-db-loader --> table: " + table + ", threads: " + numberOfThreads + ", num. files: " + fileNames.size());
		
		initThreadPool(numberOfThreads);
		
		int recordsRead = 0;
		for(String fileName : fileNames)
		{
			File csvData = new File(fileName);
			CSVParser parser = CSVParser.parse(csvData,Charset.defaultCharset(), CSVFormat.RFC4180.withHeader(header.split(",")));
			for (CSVRecord csvRecord : parser) 
			{
				poolExecutor.execute(new DynamoDBPutTask(table,csvRecord.toMap()));
				recordsRead++;
				
				if(recordsRead % 500 == 0)
				{
					logger.info("file: " + fileName + ", " + recordsRead + " records have been processed.");
				}
				
			}
			logger.info("file: " + fileName + ", has been processed.");
		}
		
		System.exit(0);
	}
	
	private static Namespace parseArgs(String args[])
	{
		ArgumentParser parser = ArgumentParsers.newArgumentParser("dynamo-db-loader")
                .defaultHelp(true)
                .description("Loads csv files to the target table in Dynamo DB");
		
        parser.addArgument("-p","--threads")
        	.type(Integer.class)
        	.setDefault(10)
        	.help("The number of threads that will do the work in case there are more than one file to process.");
		
        parser.addArgument("-t", "--table")
        	.type(String.class)
        	.required(true)
        	.help("Specify the table to where the data will be loaded.");
        	
        parser.addArgument("-d", "--header")
        	.type(String.class)
        	.required(true)
        	.help("Headers of the csv in comma separated, ex: Key,City,Country,Date");
        
        parser.addArgument("file")
        	.nargs("*")
        	.help("CSV dump file.");
        
        Namespace ns = null;
        try 
        {
            ns = parser.parseArgs(args);
        } 
        catch (ArgumentParserException e) 
        {
            parser.handleError(e);
            System.exit(1);
        }
        
        return ns;
	}
	
	private static void initThreadPool(int corePoolSize)
	{
		poolExecutor = new NotifyingBlockingThreadPoolExecutor(corePoolSize, corePoolSize, KEEP_ALIVE_TIME, TimeUnit.SECONDS, BLOCKING_TIMEOUT, TimeUnit.SECONDS, new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				logger.info( "*** ... Still waiting to process new tasks, all the threads are busy.***");
				return true; // keep waiting
			}
		},new MyThreadFactory(DYNAMO_DB_LOADER_POOL));
	}
	
}
