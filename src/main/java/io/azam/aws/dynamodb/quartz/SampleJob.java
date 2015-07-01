package io.azam.aws.dynamodb.quartz;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class SampleJob implements Job {
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		long now = System.currentTimeMillis();
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println(f.format(new Date(now)));
	}

	public static void main(String[] args) {
		JobDetail j = JobBuilder.newJob(SampleJob.class)
				.withIdentity("sampleJob", "group").build();
		CronTrigger t = TriggerBuilder
				.newTrigger()
				.withIdentity("sampleTrigger", "group")
				.withSchedule(
						CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
				.forJob("sampleJob", "group").build();

		Scheduler s = null;
		try {
			s = new StdSchedulerFactory().getScheduler();
			s.start();
			s.clear();
			if (!s.checkExists(new JobKey("sampleJob", "group"))) {
				s.scheduleJob(j, t);
			}
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
