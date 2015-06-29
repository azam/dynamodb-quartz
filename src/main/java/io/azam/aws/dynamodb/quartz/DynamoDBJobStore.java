package io.azam.aws.dynamodb.quartz;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.quartz.impl.matchers.StringMatcher.StringOperatorName;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.quartz.utils.Key;
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
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * {@link org.quartz.spi.JobStore} implementation for DynamoDB
 *
 * @author Azamshul Azizy
 */
public class DynamoDBJobStore implements JobStore {
	// Defaults
	public static final String DEFAULT_ENDPOINT = "http://localhost:8000";
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
	public static final String KEY_STATE = "state";
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

	// Trigger types
	public static final String TRIGGERTYPE_CRON = "cron";
	public static final String TRIGGERTYPE_SIMPLE = "simple";
	public static final String TRIGGERTYPE_UNKNOWN = "unknown";

	// Job state
	public static final String JOBSTATE_PAUSED = "paused";

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
	private String prefix = null;
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
		this.log.trace("initialize");
		this.loadHelper = loadHelper;
		this.signaler = signaler;
		AWSCredentialsProviderChain auth = new AWSCredentialsProviderChain(
				new EnvironmentVariableCredentialsProvider(),
				new SystemPropertiesCredentialsProvider(),
				new ProfileCredentialsProvider(),
				new InstanceProfileCredentialsProvider());
		this.client = new AmazonDynamoDBClient(auth);
		if (this.useEndpoint) {
			this.log.info("Using endpoint: " + this.endpoint);
			this.client.withEndpoint(this.endpoint);
		} else {
			this.log.info("Using region: " + this.region.getName());
			this.client.withRegion(this.region);
		}
		init();
	}

	@Override
	public synchronized void schedulerStarted() throws SchedulerException {
		this.log.trace("schedulerStarted");
		// TODO
		this.schedulerState = SCHEDULERSTATE_RUNNING;
	}

	@Override
	public synchronized void schedulerPaused() {
		this.log.trace("schedulerPaused");
		this.schedulerState = SCHEDULERSTATE_PAUSED;
	}

	@Override
	public synchronized void schedulerResumed() {
		this.log.trace("schedulerResumed");
		this.schedulerState = SCHEDULERSTATE_RUNNING;
	}

	@Override
	public synchronized void shutdown() {
		this.log.trace("shutdown");
		this.schedulerState = SCHEDULERSTATE_STOPPED;
	}

	@Override
	public boolean supportsPersistence() {
		this.log.trace("supportsPersistence");
		return true;
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		this.log.trace("getEstimatedTimeToReleaseAndAcquireTrigger");
		return this.triggerEstimate;
	}

	@Override
	public boolean isClustered() {
		this.log.trace("isClustered");
		return true;
	}

	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		this.log.trace("storeJobAndTrigger");
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
		this.log.trace("storeJob");
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
		this.log.trace("storeJobsAndTriggers");
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
		this.log.trace("removeJob");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		DeleteItemRequest req = new DeleteItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true));
		try {
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
		this.log.trace("removeJobs");
		// TODO: Use batch write
		boolean removed = true;
		for (JobKey k : jobKeys) {
			removed = removeJob(k) ? removed : false;
		}
		return removed;
	}

	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		this.log.trace("retrieveJob");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				return itemToJob(item);
			}
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		this.log.trace("storeTrigger");
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
		this.log.trace("removeTrigger");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		DeleteItemRequest req = new DeleteItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true));
		try {
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
		this.log.trace("removeTriggers");
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
		this.log.trace("replaceTrigger");
		// TODO: fetch old jobkey
		storeTrigger(newTrigger, true);
		return false;
	}

	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		this.log.trace("retrieveTrigger");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				return itemToTrigger(item);
			}
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		this.log.trace("checkExists");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.withAttributesToGet(KEY_KEY);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				return true;
			}
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return false;
	}

	@Override
	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		this.log.trace("checkExists");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withAttributesToGet(KEY_KEY);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				return true;
			}
		} catch (ConditionalCheckFailedException e) {
			this.log.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return false;
	}

	@Override
	public void clearAllSchedulingData() throws JobPersistenceException {
		this.log.trace("clearAllSchedulingData");
		// TODO Auto-generated method stub
	}

	@Override
	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		this.log.trace("storeCalendar");
		// TODO Auto-generated method stub

	}

	@Override
	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		this.log.trace("removeCalendar");
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		this.log.trace("retrieveCalendar");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		this.log.trace("getNumberOfJobs");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameJobs);
		req.withAttributesToGet(KEY_KEY);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			int count = 0;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				count += res.getCount();
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return count;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		this.log.trace("getNumberOfTriggers");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.withAttributesToGet(KEY_KEY);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			int count = 0;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				count += res.getCount();
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return count;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		this.log.trace("getNumberOfCalendars");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameCalendars);
		req.withAttributesToGet(KEY_NAME);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			int count = 0;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				count += res.getCount();
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return count;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		this.log.trace("getJobKeys");
		StringOperatorName op = matcher.getCompareWithOperator();
		String val = matcher.getCompareToValue();
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameJobs);
		req.withAttributesToGet(KEY_KEY, KEY_NAME, KEY_GROUP);
		switch (op) {
		case ANYTHING:
			break;
		case CONTAINS:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.CONTAINS)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case ENDS_WITH:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.CONTAINS)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case EQUALS:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.EQ)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case STARTS_WITH:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.BEGINS_WITH)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		default:
			throw new JobPersistenceException("Invalid matcher");
		}
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<JobKey> keys = new HashSet<JobKey>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					if (op == StringOperatorName.ENDS_WITH
							&& !item.get(KEY_GROUP).getS().endsWith(val)) {
						continue;
					}
					keys.add(new JobKey(item.get(KEY_NAME).getS(), item.get(
							KEY_GROUP).getS()));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return keys;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		this.log.trace("getTriggerKeys");
		StringOperatorName op = matcher.getCompareWithOperator();
		String val = matcher.getCompareToValue();
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.withAttributesToGet(KEY_KEY, KEY_NAME, KEY_GROUP);
		switch (op) {
		case ANYTHING:
			break;
		case CONTAINS:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.CONTAINS)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case ENDS_WITH:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.CONTAINS)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case EQUALS:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.EQ)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		case STARTS_WITH:
			req.addScanFilterEntry(KEY_GROUP, new Condition()
					.withComparisonOperator(ComparisonOperator.BEGINS_WITH)
					.withAttributeValueList(new AttributeValue(val)));
			break;
		default:
			throw new JobPersistenceException("Invalid matcher");
		}
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<TriggerKey> keys = new HashSet<TriggerKey>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					if (op == StringOperatorName.ENDS_WITH
							&& !item.get(KEY_GROUP).getS().endsWith(val)) {
						continue;
					}
					keys.add(new TriggerKey(item.get(KEY_NAME).getS(), item
							.get(KEY_GROUP).getS()));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return keys;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException {
		this.log.trace("getJobGroupNames");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameJobs);
		req.withAttributesToGet(KEY_KEY, KEY_GROUP);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<String> groups = new HashSet<String>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					groups.add(item.get(KEY_GROUP).getS());
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		this.log.trace("getTriggerGroupNames");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.withAttributesToGet(KEY_KEY, KEY_GROUP);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<String> groups = new HashSet<String>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					groups.add(item.get(KEY_GROUP).getS());
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getCalendarNames() throws JobPersistenceException {
		this.log.trace("getCalendarNames");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameCalendars);
		req.withAttributesToGet(KEY_NAME);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<String> groups = new HashSet<String>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					groups.add(item.get(KEY_NAME).getS());
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		this.log.trace("getTriggersForJob");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.addScanFilterEntry(
				KEY_JOB,
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue(formatKey(jobKey))));
		try {
			boolean hasMore = true;
			ScanResult res = null;
			List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					try {
						triggers.add(itemToTrigger(item));
					} catch (ClassNotFoundException e) {
						this.log.error(e.getMessage(), e);
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return triggers;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		this.log.trace("getTriggerState");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withAttributesToGet(KEY_KEY, KEY_STATE);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty() && item.containsKey(KEY_STATE)) {
				String state = strValue(item, KEY_STATE);
				return TriggerState.valueOf(state);
			}
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return TriggerState.NONE;
	}

	@Override
	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		this.log.trace("pauseTrigger");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.PAUSED.name()),
				AttributeAction.PUT));
		// TODO: add condition if trigger is pausable
		try {
			UpdateItemResult res = this.client.updateItem(req);
			Map<String, AttributeValue> oldItem = res.getAttributes();
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		this.log.trace("pauseTriggers");
		Collection<String> groups = new HashSet<String>();
		Set<TriggerKey> keys = getTriggerKeys(matcher);
		for (TriggerKey k : keys) {
			pauseTrigger(k);
			groups.add(k.getGroup());
		}
		return groups;
	}

	@Override
	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		this.log.trace("pauseJob");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.PAUSED.name()),
				AttributeAction.PUT));
		// TODO: add condition if job is pausable
		try {
			UpdateItemResult res = this.client.updateItem(req);
			Map<String, AttributeValue> oldItem = res.getAttributes();
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		this.log.trace("pauseJobs");
		Collection<String> groups = new HashSet<String>();
		Set<JobKey> keys = getJobKeys(groupMatcher);
		for (JobKey k : keys) {
			pauseJob(k);
			groups.add(k.getGroup());
		}
		return groups;
	}

	@Override
	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		this.log.trace("resumeTrigger");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withReturnValues(ReturnValue.ALL_NEW);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.NORMAL.name()),
				AttributeAction.PUT));
		// TODO: add condition if trigger is resumable
		try {
			UpdateItemResult res = this.client.updateItem(req);
			Map<String, AttributeValue> item = res.getAttributes();
			OperableTrigger t = itemToTrigger(item);
			misfireCheck(t);
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		this.log.trace("resumeTriggers");
		Collection<String> groups = new HashSet<String>();
		Set<TriggerKey> keys = getTriggerKeys(matcher);
		for (TriggerKey k : keys) {
			resumeTrigger(k);
			groups.add(k.getGroup());
		}
		return groups;
	}

	@Override
	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		this.log.trace("getPausedTriggerGroups");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.withAttributesToGet(KEY_KEY, KEY_GROUP, KEY_STATE);
		req.addScanFilterEntry(
				KEY_STATE,
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue(TriggerState.PAUSED.name())));
		// TODO: should we only return groups where all triggers are paused?
		try {
			boolean hasMore = true;
			ScanResult res = null;
			Set<String> groups = new HashSet<String>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					groups.add(strValue(item, KEY_GROUP));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return groups;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		this.log.trace("resumeJob");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		// TODO: add condition if job is pausable
		try {
			UpdateItemResult res = this.client.updateItem(req);
			Map<String, AttributeValue> oldItem = res.getAttributes();
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		List<TriggerKey> triggerKeys = getTriggerKeysForJob(jobKey);
		for (TriggerKey k : triggerKeys) {
			resumeTrigger(k);
		}
	}

	@Override
	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		this.log.trace("resumeJobs");
		Collection<String> groups = new HashSet<String>();
		Set<JobKey> keys = getJobKeys(matcher);
		for (JobKey k : keys) {
			resumeJob(k);
			groups.add(k.getGroup());
		}
		return groups;
	}

	@Override
	public void pauseAll() throws JobPersistenceException {
		this.log.trace("pauseAll");
		Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.anyTriggerGroup());
		for (TriggerKey k : keys) {
			pauseTrigger(k);
		}
	}

	@Override
	public void resumeAll() throws JobPersistenceException {
		this.log.trace("resumeAll");
		Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.anyTriggerGroup());
		for (TriggerKey k : keys) {
			resumeTrigger(k);
		}
	}

	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		this.log.trace("acquireNextTriggers");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.addScanFilterEntry(
				KEY_STATE,
				new Condition()
						.withComparisonOperator(ComparisonOperator.NE)
						.withAttributeValueList(
								new AttributeValue(TriggerState.COMPLETE.name())));
		req.addScanFilterEntry(
				KEY_STATE,
				new Condition().withComparisonOperator(ComparisonOperator.NE)
						.withAttributeValueList(
								new AttributeValue(TriggerState.PAUSED.name())));
		List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
		Set<TriggerKey> completed = new HashSet<TriggerKey>();
		try {
			boolean hasMore = true;
			ScanResult res = null;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					try {
						triggers.add(itemToTrigger(item));
					} catch (ClassNotFoundException e) {
						this.log.error(e.getMessage(), e);
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		List<OperableTrigger> acquired = new ArrayList<OperableTrigger>();
		for (OperableTrigger t : triggers) {
			if (misfireCheck(t)) {
				continue;
			}
			if (t.getNextFireTime() == null) {
				completed.add(t.getKey());
				continue;
			}
			// TODO: set trigger.state = acquired
			// TODO: set trigger.lockedby = instance id
			// TODO: set trigger.lockedat = now
			// TODO: acquired.add(t);
		}
		return acquired;
	}

	@Override
	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		this.log.trace("releaseAcquiredTrigger");
		// TODO Auto-generated method stub

	}

	@Override
	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
		this.log.trace("triggersFired");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		this.log.trace("triggeredJobComplete");
		// TODO Auto-generated method stub

	}

	@Override
	public void setInstanceId(String instanceId) {
		this.log.trace("setInstanceId");
		this.instanceId = instanceId;
	}

	@Override
	public void setInstanceName(String instanceName) {
		this.log.trace("setInstanceName");
		this.instanceName = instanceName;
	}

	@Override
	public void setThreadPoolSize(int poolSize) {
		this.log.trace("setThreadPoolSize");
		this.poolSize = poolSize;
	}

	public void setPrefix(String prefix) {
		this.log.trace("setPrefix");
		this.prefix = prefix;
		if (this.prefix != null && !this.prefix.isEmpty()) {
			this.tableNameCalendars = this.prefix + "_"
					+ this.tableNameCalendars;
			this.tableNameJobs = this.prefix + "_" + this.tableNameJobs;
			this.tableNameTriggers = this.prefix + "_" + this.tableNameTriggers;
		}
	}

	public void setRegion(String region) {
		this.log.trace("setRegion");
		this.region = Region.getRegion(Regions.fromName(region));
	}

	public void setEndpoint(String endpoint) {
		this.log.trace("setEndpoint");
		this.endpoint = endpoint;
		if (this.endpoint != null && !this.endpoint.isEmpty()) {
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

	private static String formatKey(Key<?> k) {
		return k.getGroup() + ":" + k.getName();
	}

	private static JobKey parseJobKey(String k) {
		if (k != null && !k.isEmpty()) {
			String[] p = k.split(":");
			if (p.length >= 2) {
				return new JobKey(p[1], p[0]);
			}
		}
		return null;
	}

	private static TriggerKey parseTriggerKey(String k) {
		if (k != null && !k.isEmpty()) {
			String[] p = k.split(":");
			if (p.length >= 2) {
				return new TriggerKey(p[1], p[0]);
			}
		}
		return null;
	}

	private void init() {
		if (!Tables.doesTableExist(this.client, this.tableNameCalendars)) {
			this.log.error("Creating table: " + this.tableNameCalendars);
			this.client
					.createTable(Arrays.asList(new AttributeDefinition()
							.withAttributeName(KEY_NAME).withAttributeType(
									ScalarAttributeType.S)),
							this.tableNameCalendars, Arrays
									.asList(new KeySchemaElement()
											.withAttributeName(KEY_NAME)
											.withKeyType(KeyType.HASH)),
							new ProvisionedThroughput(2L, 2L));
		}
		if (!Tables.doesTableExist(this.client, this.tableNameJobs)) {
			this.log.error("Creating table: " + this.tableNameJobs);
			this.client.createTable(
					Arrays.asList(new AttributeDefinition().withAttributeName(
							KEY_KEY).withAttributeType(ScalarAttributeType.S)),
					this.tableNameJobs,
					Arrays.asList(new KeySchemaElement().withAttributeName(
							KEY_KEY).withKeyType(KeyType.HASH)),
					new ProvisionedThroughput(2L, 2L));
		}
		if (!Tables.doesTableExist(this.client, this.tableNameTriggers)) {
			this.log.error("Creating table: " + this.tableNameTriggers);
			this.client.createTable(
					Arrays.asList(new AttributeDefinition().withAttributeName(
							KEY_KEY).withAttributeType(ScalarAttributeType.S)),
					this.tableNameTriggers,
					Arrays.asList(new KeySchemaElement().withAttributeName(
							KEY_KEY).withKeyType(KeyType.HASH)),
					new ProvisionedThroughput(2L, 2L));
		}
		try {
			Tables.awaitTableToBecomeActive(this.client,
					this.tableNameCalendars, 60000, 1000);
			Tables.awaitTableToBecomeActive(this.client,
					this.tableNameTriggers, 60000, 1000);
			Tables.awaitTableToBecomeActive(this.client, this.tableNameJobs,
					60000, 1000);
		} catch (InterruptedException e) {
			this.log.error(e.getMessage(), e);
			this.shutdown();
		}
	}

	private long misfireThreshold() {
		return this.triggerEstimate;
	}

	private boolean misfireCheck(OperableTrigger t)
			throws JobPersistenceException {
		long misfireTime = System.currentTimeMillis();
		if (misfireThreshold() > 0) {
			misfireTime -= misfireThreshold();
		}
		Date next = t.getNextFireTime();
		if (next == null
				|| next.getTime() > misfireTime
				|| t.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
			return false;
		}
		Calendar cal = null;
		if (t.getCalendarName() != null && !t.getCalendarName().isEmpty()) {
			cal = retrieveCalendar(t.getCalendarName());
		}
		this.signaler.notifyTriggerListenersMisfired((Trigger) t.clone());
		t.updateAfterMisfire(cal);
		if (next.equals(t.getNextFireTime())) {
			return false;
		}
		try {
			storeTrigger(t, true);
		} catch (ObjectAlreadyExistsException e) {
			// absorb
		}
		if (t.getNextFireTime() == null) { // Trigger completed
			completeTrigger(t.getKey());
			this.signaler.notifySchedulerListenersFinalized(t);
		}
		return true;
	}

	private void completeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.COMPLETE.name()),
				AttributeAction.PUT));
		// TODO: add condition if trigger is completable
		try {
			UpdateItemResult res = this.client.updateItem(req);
			Map<String, AttributeValue> item = res.getAttributes();
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private List<TriggerKey> getTriggerKeysForJob(JobKey jobKey)
			throws JobPersistenceException {
		this.log.trace("getTriggersForJob");
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.withAttributesToGet(KEY_KEY, KEY_JOB);
		req.addScanFilterEntry(
				KEY_JOB,
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue(formatKey(jobKey))));
		try {
			boolean hasMore = true;
			ScanResult res = null;
			List<TriggerKey> triggers = new ArrayList<TriggerKey>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				for (Map<String, AttributeValue> item : res.getItems()) {
					triggers.add(parseTriggerKey(strValue(item, KEY_KEY)));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return triggers;
		} catch (AmazonServiceException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			this.log.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private Map<String, AttributeValue> jobToItem(JobDetail j) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		attr(item, KEY_KEY, formatKey(j.getKey()));
		attr(item, KEY_GROUP, j.getKey().getGroup());
		attr(item, KEY_NAME, j.getKey().getName());
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
		attr(item, KEY_KEY, formatKey(t.getKey()));
		attr(item, KEY_GROUP, t.getKey().getGroup());
		attr(item, KEY_NAME, t.getKey().getName());
		attr(item, KEY_JOB, formatKey(t.getJobKey()));
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
			attr(item, KEY_TYPE, TRIGGERTYPE_SIMPLE);
			attr(item, KEY_COUNT, ((SimpleTrigger) t).getRepeatCount());
			attr(item, KEY_INTERVAL, ((SimpleTrigger) t).getRepeatInterval());
			attr(item, KEY_TIMES, ((SimpleTrigger) t).getTimesTriggered());
		} else if (t instanceof CronTrigger) {
			attr(item, KEY_TYPE, TRIGGERTYPE_CRON);
			attr(item, KEY_CRON, ((CronTrigger) t).getCronExpression());
			attr(item, KEY_TIMEZONE, ((CronTrigger) t).getTimeZone());
		} else {
			attr(item, KEY_TYPE, TRIGGERTYPE_UNKNOWN);
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

	private static Map<String, Object> itemToMap(
			Map<String, AttributeValue> item) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (String k : item.keySet()) {
			AttributeValue v = item.get(k);
			if (v.isNULL()) {
			} else if (v.isBOOL()) {
				map.put(k, v.getBOOL());
			} else {
				map.put(k, v.getS());
			}
		}
		return map;
	}

	private JobDetail itemToJob(Map<String, AttributeValue> item)
			throws ClassNotFoundException {
		JobKey key = new JobKey(item.get(KEY_KEY).getS());
		Class<Job> cls = (Class<Job>) this.loadHelper.getClassLoader()
				.loadClass(item.get(KEY_CLASS).getS());
		JobBuilder builder = JobBuilder.newJob(cls).withIdentity(key)
				.withDescription(item.get(KEY_DESCRIPTION).getS())
				.storeDurably(item.get(KEY_DURABLE).getBOOL());
		Map<String, AttributeValue> m = item.get(KEY_DATA).getM();
		if (m != null && !m.isEmpty()) {
			builder.usingJobData(new JobDataMap(itemToMap(m)));
		}
		return builder.build();
	}

	private OperableTrigger itemToTrigger(Map<String, AttributeValue> item)
			throws ClassNotFoundException {
		OperableTrigger t = null;
		String type = item.get(KEY_TYPE).getS();
		if (type != null) {
			if (TRIGGERTYPE_SIMPLE.equalsIgnoreCase(type)) {
				t = new SimpleTriggerImpl();
				Integer c = intValue(item, KEY_COUNT);
				if (c != null) {
					((SimpleTriggerImpl) t).setRepeatCount(c);
				}
				Long i = longValue(item, KEY_INTERVAL);
				if (i != null) {
					((SimpleTriggerImpl) t).setRepeatInterval(c);
				}
				Integer x = intValue(item, KEY_TIMES);
				if (x != null) {
					((SimpleTriggerImpl) t).setTimesTriggered(x);
				}
			} else if (TRIGGERTYPE_CRON.equalsIgnoreCase(type)) {
				t = new CronTriggerImpl();
				String c = strValue(item, KEY_CRON);
				if (c != null && !c.isEmpty()) {
					try {
						((CronTriggerImpl) t).setCronExpression(c);
					} catch (ParseException e) {
						this.log.error(e.getMessage(), e);
					}
				}
				String tz = strValue(item, KEY_TIMEZONE);
				if (tz != null && !tz.isEmpty()) {
					((CronTriggerImpl) t).setTimeZone(TimeZone.getTimeZone(tz));
				}
			}
		}
		if (t != null) {
			t.setKey(parseTriggerKey(strValue(item, KEY_KEY)));
			t.setJobKey(parseJobKey(strValue(item, KEY_JOB)));
			t.setKey(parseTriggerKey(strValue(item, KEY_KEY)));
			t.setDescription(strValue(item, KEY_DESCRIPTION));
			t.setFireInstanceId(strValue(item, KEY_INSTANCE));
			t.setCalendarName(strValue(item, KEY_CALENDAR));
			t.setPriority(intValue(item, KEY_DESCRIPTION));
			t.setMisfireInstruction(intValue(item, KEY_MISFIRE));
			t.setStartTime(dateValue(item, KEY_START));
			t.setEndTime(dateValue(item, KEY_END));
			t.setNextFireTime(dateValue(item, KEY_NEXT));
			t.setPreviousFireTime(dateValue(item, KEY_PREV));
		}
		return t;
	}

	private static String strValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			return value.getS();
		}
		return null;
	}

	private static Long longValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			String n = value.getN();
			if (n != null && !n.isEmpty()) {
				return Long.valueOf(n, 10);
			}
		}
		return null;
	}

	private static Integer intValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			String n = value.getN();
			if (n != null && !n.isEmpty()) {
				return Integer.valueOf(n, 10);
			}
		}
		return null;
	}

	private static Date dateValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			String n = value.getN();
			if (n != null && !n.isEmpty()) {
				return new Date(Long.valueOf(n, 10));
			}
		}
		return null;
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
