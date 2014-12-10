package mr.jc.util;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Monitors the thread pools and prints the statistics
 * @author jonataschagas
 *
 */
public class ThreadPoolMonitor implements Runnable
{
	private ThreadPoolExecutor executor;
    private String poolName;
	private int seconds;
    private boolean run=true;
 
    private Logger logger = LogManager.getLogger(ThreadPoolMonitor.class);
    
    public ThreadPoolMonitor(ThreadPoolExecutor executor, int delay,String poolName)
    {
        this.executor = executor;
        this.seconds = delay;
        this.poolName = poolName;
    }
     
    public void shutdown(){
        this.run=false;
    }
 
    @Override
    public void run()
    {
        while(run){
                logger.info(
                    String.format("[monitor - %s] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                        this.poolName,
                    	this.executor.getPoolSize(),
                        this.executor.getCorePoolSize(),
                        this.executor.getActiveCount(),
                        this.executor.getCompletedTaskCount(),
                        this.executor.getTaskCount(),
                        this.executor.isShutdown(),
                        this.executor.isTerminated()));
                try {
                    Thread.sleep(seconds*1000);
                } catch (InterruptedException e) {
                    logger.error("Error in the monitoring thread.",e);
                }
        }
             
    }
}
