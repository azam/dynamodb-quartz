package io.azam.aws.dynamodb.quartz;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@DisallowConcurrentExecution()
public class DisallowConcurrentJob implements Job {
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		long now = System.currentTimeMillis();
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println("DisallowConcurrentJob: " + f.format(new Date(now)));
	}
}
