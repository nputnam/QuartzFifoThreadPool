package com.quartzspi;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class PrintJob implements Job {

    private static final Logger log = LogManager.getLogger(PrintJob.class);

    public static final String NUM_EXECUTIONS = "NumExecutions";

    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        Random r = new Random();
             JobDataMap map = jobExecutionContext.getJobDetail().getJobDataMap();
         int executeCount = 0;
        if (map.containsKey(NUM_EXECUTIONS)) {
            executeCount = map.getInt(NUM_EXECUTIONS);
        }

        executeCount++;
        map.put(NUM_EXECUTIONS, executeCount);

        try {
            int i = r.nextInt(60);
            log.info("Running pring job "+jobExecutionContext.getJobDetail().getKey()+" for "+i+" seconds. run "+jobExecutionContext.getJobDetail().getJobDataMap().getInt(NUM_EXECUTIONS));
            Thread.sleep(TimeUnit.SECONDS.toMillis(i));
        } catch (InterruptedException e) {
            log.error(e);
        }
    }
}
