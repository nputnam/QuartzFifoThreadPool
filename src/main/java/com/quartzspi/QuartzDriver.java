package com.quartzspi;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzDriver {

    private static final Logger log = LogManager.getLogger(QuartzDriver.class);

    public static void main(String...args) throws Exception {

        SchedulerFactory fifoFactory = new StdSchedulerFactory();
        Scheduler fifoScheduler = fifoFactory.getScheduler();
        fifoScheduler.start();

        for (int i =0; i<100;i++)  {
            JobDetail job = JobBuilder.newJob().storeDurably().withIdentity("FIFO"+i).ofType(PrintJob.class).build();

            SimpleScheduleBuilder simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(3).repeatForever().withMisfireHandlingInstructionNextWithExistingCount();

            Trigger trigger = TriggerBuilder.newTrigger().forJob(job).startNow().withSchedule(simpleScheduleBuilder).build();

            fifoScheduler.addJob(job, false);

            fifoScheduler.scheduleJob(trigger);
        }

    }
}
