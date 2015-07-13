package io.azam.aws.dynamodb.quartz;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ConcurrentJob implements Job {
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		long now = System.currentTimeMillis();
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println("#### ConcurrentJob: " + f.format(new Date(now)));
	}
}
