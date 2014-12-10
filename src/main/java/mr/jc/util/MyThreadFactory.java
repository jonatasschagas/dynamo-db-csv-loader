package mr.jc.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MyThreadFactory implements ThreadFactory {
	
	private final String name;
    private final AtomicInteger integer = new AtomicInteger(1);
     
    public MyThreadFactory(String name) 
    {
        this.name = name;
    }
     
    /** 
     * {@inheritDoc}
     */
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name + " - " + integer.getAndIncrement());
    }

}
