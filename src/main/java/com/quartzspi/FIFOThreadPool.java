package com.quartzspi;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

import java.util.concurrent.*;

/*
Class that creates a buffer of jobs. They are executed in FIFO order. If the number of jobs to be executed exceeds
the work queue they will be dropped. The job will *eventually* get rescheduled but its best if the buffer can
hold the entire range of jobs.

Inspired by http://stackoverflow.com/questions/2994299/quartz-scheduler-theadpool
*/
public class FIFOThreadPool implements ThreadPool {

    private static final Logger log = LogManager.getLogger(FIFOThreadPool.class);

    private int threadCount = 5;
    private int queueSize = 8192;

    private String instanceName;
    private String instanceID;

    private ThreadPoolExecutor executor;
    private BlockingQueue<Runnable> queue;

    public FIFOThreadPool() {
        log.info("Creating new FIFO thread pool");
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public boolean runInThread(Runnable runnable) {
        try {
            executor.submit(runnable);
        } catch (RejectedExecutionException e) {
            log.warn("Couldn't add job to work queue. Increase the size of the work queue!!");
            return false;
        }
        return true;
    }

    // Might want to block if the work pool is near capacity.
    public int blockForAvailableThreads() {
        return 1;
    }

    public void initialize() throws SchedulerConfigException {
        queue = new ArrayBlockingQueue<Runnable>(queueSize);
        executor = new ThreadPoolExecutor(threadCount, threadCount,
                1000L, TimeUnit.SECONDS, this.queue, new WorkerThreadFactory(instanceID + "-" + instanceName));
        log.info("FIFO worker queue (" + queueSize + ") created with " + threadCount + " workers.");
    }

    public void shutdown(boolean waitForJobsToComplete) {
        // Clear the back jobs and wait 10 minutes for the ones in flight to finish
        if (waitForJobsToComplete) {
            queue.clear();
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String msg = "Error waiting for jobs to finish";
                log.error(msg, e);
            }
        } else {
            executor.shutdownNow();
        }
    }

    public int getPoolSize() {
        return threadCount;
    }

    public void setInstanceId(String instanceID) {
        this.instanceID = instanceID;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    // Named thread factory for easier debugging.
    private class WorkerThreadFactory implements ThreadFactory {

        private final String instanceNameAndId;

        private WorkerThreadFactory(String instanceNameAndId) {
            this.instanceNameAndId = instanceNameAndId;
        }

        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, instanceNameAndId);
        }
    }

}
