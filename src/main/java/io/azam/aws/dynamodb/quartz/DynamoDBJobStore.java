package io.azam.aws.dynamodb.quartz;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;

/**
 * {@link org.quartz.core.JobStore} implementation for DynamoDB
 *
 * @author Azamshul Azizy
 */
public class DynamoDBJobStore implements JobStore {
	// Defaults
	public static final String DEFAULT_ENDPOINT = "http://localhost:8080";
	public static final String DEFAULT_JOBS = "jobs";
	public static final String DEFAULT_CALENDARS = "calendars";
	public static final String DEFAULT_TRIGGERS = "triggers";
	public static final int DEFAULT_POOLSIZE = 10;
	public static final long DEFAULT_TRIGGERESTIMATE = 200L;

	// Keys
	public static final String KEY_KEY = "key";
	public static final String KEY_CLASS = "class";
	public static final String KEY_GROUP = "group";
	public static final String KEY_NAME = "name";
	public static final String KEY_TYPE = "type";
	public static final String KEY_DESCRIPTION = "description";
	public static final String KEY_DURABLE = "durable";
	public static final String KEY_CONCURRENT = "concurrent";
	public static final String KEY_PERSIST = "persist";
	public static final String KEY_DATA = "data";
	public static final String KEY_JOB = "job";
	public static final String KEY_NEXT = "next";
	public static final String KEY_PREV = "prev";
	public static final String KEY_PRIORITY = "priority";
	public static final String KEY_START = "start";
	public static final String KEY_END = "end";
	public static final String KEY_FINAL = "final";
	public static final String KEY_INSTANCE = "instance";
	public static final String KEY_MISFIRE = "misfire";
	public static final String KEY_CALENDAR = "calendar";
	public static final String KEY_CRON = "cron";
	public static final String KEY_TIMEZONE = "timezone";
	public static final String KEY_COUNT = "count";
	public static final String KEY_INTERVAL = "interval";
	public static final String KEY_TIMES = "times";

	// Scheduler states
	public static final int SCHEDULERSTATE_INITIALIZED = 0;
	public static final int SCHEDULERSTATE_RUNNING = 1;
	public static final int SCHEDULERSTATE_STOPPED = 2;
	public static final int SCHEDULERSTATE_STOPPING = 3;
	public static final int SCHEDULERSTATE_PAUSED = 4;
	public static final int SCHEDULERSTATE_PAUSING = 5;
	public static final int SCHEDULERSTATE_RESUMED = 8;
	public static final int SCHEDULERSTATE_RESUMING = 9;

	// Limits
	public static final int DYNAMODB_MAXBATCHWRITE = 25;
	public static final int DYNAMODB_MAXBATCHGET = 200;

	// Instance variables
	private final Logger log = LoggerFactory.getLogger(getClass());
	private ClassLoadHelper loadHelper;
	private SchedulerSignaler signaler;
	private AmazonDynamoDBClient client;
	private String tableNameJobs = DEFAULT_JOBS;
	private String tableNameCalendars = DEFAULT_CALENDARS;
	private String tableNameTriggers = DEFAULT_TRIGGERS;
	private Region region = Region.getRegion(Regions.US_WEST_1);
	private boolean useEndpoint = false;
	private String endpoint = DEFAULT_ENDPOINT;
	private String instanceId;
	private String instanceName;
	private int schedulerState = SCHEDULERSTATE_STOPPED;
	private int poolSize = DEFAULT_POOLSIZE;
	private long triggerEstimate = DEFAULT_TRIGGERESTIMATE;

	@Override
	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		this.loadHelper = loadHelper;
		this.signaler = signaler;
		AWSCredentialsProviderChain auth = new AWSCredentialsProviderChain(
				new EnvironmentVariableCredentialsProvider(),
				new SystemPropertiesCredentialsProvider(),
				new ProfileCredentialsProvider(),
				new InstanceProfileCredentialsProvider());
		this.client = new AmazonDynamoDBClient(auth);
		if (this.useEndpoint) {
			this.client.withEndpoint(this.endpoint);
		} else {
			this.client.withRegion(this.region);
		}
	}

	@Override
	public synchronized void schedulerStarted() throws SchedulerException {
		// TODO
		this.schedulerState = SCHEDULERSTATE_RUNNING;
	}

	@Override
	public synchronized void schedulerPaused() {
		this.schedulerState = SCHEDULERSTATE_PAUSED;
	}

	@Override
	public synchronized void schedulerResumed() {
		this.schedulerState = SCHEDULERSTATE_RUNNING;
	}

	@Override
	public synchronized void shutdown() {
		this.schedulerState = SCHEDULERSTATE_STOPPED;
	}

	@Override
	public boolean supportsPersistence() {
		return true;
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		return this.triggerEstimate;
	}

	@Override
	public boolean isClustered() {
		return true;
	}

	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		try {
			storeJob(newJob, false);
		} catch (ObjectAlreadyExistsException e) {
			throw e;
		} catch (JobPersistenceException e) {
			throw e;
		}
		try {
			storeTrigger(newTrigger, false);
		} catch (ObjectAlreadyExistsException e) {
			removeJob(newJob.getKey());
			throw e;
		} catch (JobPersistenceException e) {
			removeJob(newJob.getKey());
			throw e;
		}
	}

	@Override
	public void storeJob(JobDetail newJob, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		PutItemRequest req = new PutItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withItem(jobToItem(newJob));
		if (!replaceExisting) {
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(false));
		}
		try {
			PutItemResult res = this.client.putItem(req);
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
			throw new ObjectAlreadyExistsException(newJob);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		Set<JobKey> jobKeys = new HashSet<JobKey>();
		Set<TriggerKey> triggerKeys = new HashSet<TriggerKey>();
		try {
			for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs
					.entrySet()) {
				storeJob(entry.getKey(), replace);
				jobKeys.add(entry.getKey().getKey());
				for (Trigger t : entry.getValue()) {
					storeTrigger((OperableTrigger) t, replace);
					triggerKeys.add(t.getKey());
				}
			}
		} catch (ObjectAlreadyExistsException e) {
			for (TriggerKey k : triggerKeys) {
				removeTrigger(k);
			}
			for (JobKey k : jobKeys) {
				removeJob(k);
			}
			throw e;
		} catch (JobPersistenceException e) {
			for (TriggerKey k : triggerKeys) {
				removeTrigger(k);
			}
			for (JobKey k : jobKeys) {
				removeJob(k);
			}
			throw e;
		}
	}

	@Override
	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		try {
			Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
			km.put(KEY_KEY, new AttributeValue(jobKey.toString()));
			DeleteItemRequest req = new DeleteItemRequest();
			req.withTableName(this.tableNameJobs);
			req.withKey(km);
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true));
			DeleteItemResult res = this.client.deleteItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
			return false;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		// TODO: Use batch write
		boolean removed = true;
		for (JobKey k : jobKeys) {
			removed = removeJob(k) ? removed : false;
		}
		return removed;
	}

	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		PutItemRequest req = new PutItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withItem(triggerToItem(newTrigger));
		if (!replaceExisting) {
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(false));
		}
		try {
			PutItemResult res = this.client.putItem(req);
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
			throw new ObjectAlreadyExistsException(newTrigger);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		try {
			Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
			km.put(KEY_KEY, new AttributeValue(triggerKey.toString()));
			DeleteItemRequest req = new DeleteItemRequest();
			req.withTableName(this.tableNameTriggers);
			req.withKey(km);
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true));
			DeleteItemResult res = this.client.deleteItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
			return false;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		// TODO: Use batch write
		boolean removed = true;
		for (TriggerKey k : triggerKeys) {
			removed = removeTrigger(k) ? removed : false;
		}
		return removed;
	}

	@Override
	public boolean replaceTrigger(TriggerKey triggerKey,
			OperableTrigger newTrigger) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearAllSchedulingData() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getCalendarNames() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pauseAll() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public void resumeAll() throws JobPersistenceException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	@Override
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	@Override
	public void setThreadPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public void setRegion(String region) {
		this.region = Region.getRegion(Regions.fromName(region));
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
		if (endpoint != null && !endpoint.isEmpty()) {
			try {
				URL url = new URL(this.endpoint);
				this.useEndpoint = true;
			} catch (MalformedURLException e) {
				this.log.error(e.getMessage(), e);
				this.useEndpoint = false;
			}
		} else {
			this.useEndpoint = false;
		}
	}

	private Map<String, AttributeValue> jobToItem(JobDetail j) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		attr(item, KEY_KEY, j.getKey().toString());
		attr(item, KEY_CLASS, j.getJobClass().getName());
		attr(item, KEY_DESCRIPTION, j.getDescription());
		attr(item, KEY_DURABLE, j.isDurable());
		attr(item, KEY_CONCURRENT, !j.isConcurrentExectionDisallowed());
		attr(item, KEY_PERSIST, j.isPersistJobDataAfterExecution());
		attr(item, KEY_DATA, j.getJobDataMap());
		return item;
	}

	private Map<String, AttributeValue> triggerToItem(Trigger t) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		attr(item, KEY_KEY, t.getKey().toString());
		attr(item, KEY_JOB, t.getJobKey().toString());
		attr(item, KEY_CLASS, t.getClass().getName());
		attr(item, KEY_DESCRIPTION, t.getDescription());
		attr(item, KEY_PRIORITY, t.getPriority());
		attr(item, KEY_MISFIRE, t.getMisfireInstruction());
		attr(item, KEY_CALENDAR, t.getCalendarName());
		attr(item, KEY_NEXT, t.getNextFireTime());
		attr(item, KEY_PREV, t.getPreviousFireTime());
		attr(item, KEY_START, t.getStartTime());
		attr(item, KEY_END, t.getStartTime());
		attr(item, KEY_FINAL, t.getFinalFireTime());
		attr(item, KEY_DATA, t.getJobDataMap());
		// Trigger specific
		if (t instanceof SimpleTrigger) {
			attr(item, KEY_TYPE, "simple");
			attr(item, KEY_COUNT, ((SimpleTrigger) t).getRepeatCount());
			attr(item, KEY_INTERVAL, ((SimpleTrigger) t).getRepeatInterval());
			attr(item, KEY_TIMES, ((SimpleTrigger) t).getTimesTriggered());
		} else if (t instanceof CronTrigger) {
			attr(item, KEY_TYPE, "cron");
			attr(item, KEY_CRON, ((CronTrigger) t).getCronExpression());
			attr(item, KEY_TIMEZONE, ((CronTrigger) t).getTimeZone());
		} else {
			// attr(item, KEY_TYPE, "unknown");
		}
		return item;
	}

	private static Map<String, AttributeValue> mapToItem(Map<String, Object> map) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		for (String k : map.keySet()) {
			Object v = map.get(k);
			if (v instanceof String) {
				attr(item, k, (String) v);
			} else if (v instanceof Integer) {
				attr(item, k, (Integer) v);
			} else if (v instanceof Long) {
				attr(item, k, (Long) v);
			} else if (v instanceof Float) {
				attr(item, k, (Float) v);
			} else if (v instanceof Double) {
				attr(item, k, (Double) v);
			} else if (v instanceof Boolean) {
				attr(item, k, (Boolean) v);
			}
		}
		return item;
	}

	private JobDetail itemToJob(Item item) throws ClassNotFoundException {
		JobKey key = new JobKey(item.getString(KEY_KEY));
		Class<Job> cls = (Class<Job>) this.loadHelper.getClassLoader()
				.loadClass(item.getString(KEY_CLASS));
		JobBuilder builder = JobBuilder.newJob(cls).withIdentity(key)
				.withDescription(item.getString(KEY_DESCRIPTION))
				.storeDurably(item.getBoolean(KEY_DURABLE));
		Map<String, Object> m = item.getMap(KEY_DATA);
		if (m != null && !m.isEmpty()) {
			builder.usingJobData(new JobDataMap(m));
		}
		return builder.build();
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			String value) {
		if (value != null && !value.isEmpty()) {
			map.put(key, new AttributeValue(value));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Long value) {
		if (value != null) {
			map.put(key, new AttributeValue().withN(Long.toString(value, 10)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Integer value) {
		if (value != null) {
			map.put(key,
					new AttributeValue().withN(Integer.toString(value, 10)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Double value) {
		if (value != null) {
			map.put(key, new AttributeValue().withN(Double.toString(value)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Float value) {
		if (value != null) {
			map.put(key, new AttributeValue().withN(Float.toString(value)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Boolean value) {
		if (value != null) {
			map.put(key, new AttributeValue().withBOOL(value));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Map value) {
		if (value != null && !value.isEmpty()) {
			map.put(key, new AttributeValue().withM(mapToItem(value)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			Date value) {
		if (value != null) {
			map.put(key, new AttributeValue().withN(Long.toString(
					value.getTime(), 10)));
		}
	}

	private static void attr(Map<String, AttributeValue> map, String key,
			TimeZone value) {
		if (value != null) {
			map.put(key, new AttributeValue(value.getID()));
		}
	}

}
