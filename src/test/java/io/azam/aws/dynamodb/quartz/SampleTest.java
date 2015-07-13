package io.azam.aws.dynamodb.quartz;

import java.io.IOException;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class SampleTest {
	public static void main(String[] args) {
		JobDetail j1 = JobBuilder.newJob(ConcurrentJob.class)
				.withIdentity("concurrent", "group")
				.withDescription("重複可能なジョブです").build();
		CronTrigger t1 = TriggerBuilder
				.newTrigger()
				.withIdentity("concurrentTrigger", "group")
				.withDescription("重複可能なトリガーです")
				.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * * * ?"))
				.forJob("concurrent", "group").build();
		JobDetail j2 = JobBuilder.newJob(DisallowConcurrentJob.class)
				.withIdentity("disallowConcurrent", "group")
				.withDescription("重複不可能なジョブです").build();
		CronTrigger t2 = TriggerBuilder
				.newTrigger()
				.withIdentity("disallowConcurrentTrigger", "group")
				.withDescription("重複不可能なトリガーです")
				.withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * * * ?"))
				.forJob("disallowConcurrent", "group").build();
		Scheduler s = null;
		try {
			s = new StdSchedulerFactory().getScheduler();
			s.clear();
			s.scheduleJob(j1, t1);
			s.scheduleJob(j2, t2);
			s.start();
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			int k = -1;
			while (k != '\n') {
				k = System.in.read();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (s != null) {
					s.shutdown(false);
				}
			} catch (SchedulerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Shutdown OK");
		}
	}
}
