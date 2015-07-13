package io.azam.aws.dynamodb.quartz;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

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
import org.quartz.spi.TriggerFiredBundle;
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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
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
	public static final long DEFAULT_MISFIRETHRESHOLD = 60000L;
	public static final long DEFAULT_TRIGGERESTIMATE = 200L;

	// Keys
	public static final String KEY_KEY = "key";
	public static final String KEY_CLASS = "class";
	public static final String KEY_GROUP = "group";
	public static final String KEY_NAME = "name";
	public static final String KEY_STATE = "state";
	public static final String KEY_TYPE = "type";
	public static final String KEY_DESCRIPTION = "description";
	public static final String KEY_SUMMARY = "summary";
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
	public static final String KEY_LOCKED = "locked";
	public static final String KEY_LOCKEDBY = "lockedBy";
	public static final String KEY_LOCKEDAT = "lockedAt";
	public static final String KEY_BASE = "base";
	public static final String KEY_BYTECODE = "byteCode";

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

	// Class logger
	private static final Logger LOG = LoggerFactory
			.getLogger(DynamoDBJobStore.class);

	// Instance variables
	private final Object initLock = new Object();
	private ClassLoadHelper loadHelper;
	private SchedulerSignaler signaler;
	private AmazonDynamoDB client;
	private String tableNameJobs = DEFAULT_JOBS;
	private String tableNameCalendars = DEFAULT_CALENDARS;
	private String tableNameTriggers = DEFAULT_TRIGGERS;
	private String prefix = null;
	private Region region = Region.getRegion(Regions.US_WEST_1);
	private boolean useEndpoint = false;
	private boolean clustered = false;
	private String endpoint = DEFAULT_ENDPOINT;
	private String instanceId;
	@SuppressWarnings("unused")
	private String instanceName;
	@SuppressWarnings("unused")
	private int schedulerState = SCHEDULERSTATE_STOPPED;
	private long misfireThreshold = DEFAULT_MISFIRETHRESHOLD;
	@SuppressWarnings("unused")
	private int poolSize = DEFAULT_POOLSIZE;
	private long triggerEstimate = DEFAULT_TRIGGERESTIMATE;

	@Override
	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		LOG.trace("initialize");
		this.loadHelper = loadHelper;
		this.signaler = signaler;
		synchronized (this.initLock) {
			AWSCredentialsProviderChain auth = new AWSCredentialsProviderChain(
					new EnvironmentVariableCredentialsProvider(),
					new SystemPropertiesCredentialsProvider(),
					new ProfileCredentialsProvider(),
					new InstanceProfileCredentialsProvider());
			this.client = new AmazonDynamoDBClient(auth);
			if (this.useEndpoint) {
				LOG.info("Using endpoint: " + this.endpoint);
				this.client.setEndpoint(this.endpoint);
			} else {
				LOG.info("Using region: " + this.region.getName());
				this.client.setRegion(this.region);
			}
			init();
		}
	}

	@Override
	public synchronized void schedulerStarted() throws SchedulerException {
		LOG.trace("schedulerStarted");
		synchronized (this.initLock) {
			this.schedulerState = SCHEDULERSTATE_RUNNING;
		}
	}

	@Override
	public synchronized void schedulerPaused() {
		LOG.trace("schedulerPaused");
		synchronized (this.initLock) {
			this.schedulerState = SCHEDULERSTATE_PAUSED;
		}
	}

	@Override
	public synchronized void schedulerResumed() {
		LOG.trace("schedulerResumed");
		synchronized (this.initLock) {
			this.schedulerState = SCHEDULERSTATE_RUNNING;
		}
	}

	@Override
	public synchronized void shutdown() {
		LOG.trace("shutdown");
		synchronized (this.initLock) {
			this.schedulerState = SCHEDULERSTATE_STOPPED;
		}
	}

	@Override
	public boolean supportsPersistence() {
		LOG.trace("supportsPersistence");
		return true;
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		LOG.trace("getEstimatedTimeToReleaseAndAcquireTrigger");
		return this.triggerEstimate;
	}

	@Override
	public boolean isClustered() {
		LOG.trace("isClustered");
		return this.clustered;
	}

	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.trace("storeJobAndTrigger");
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
		LOG.trace("storeJob");
		storeJob(newJob, replaceExisting, null);
	}

	@Override
	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		LOG.trace("storeJobsAndTriggers");
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
			// for (TriggerKey k : triggerKeys) {
			// removeTrigger(k);
			// }
			// for (JobKey k : jobKeys) {
			// removeJob(k);
			// }
			throw e;
		} catch (JobPersistenceException e) {
			// for (TriggerKey k : triggerKeys) {
			// removeTrigger(k);
			// }
			// for (JobKey k : jobKeys) {
			// removeJob(k);
			// }
			throw e;
		}
	}

	@Override
	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		LOG.trace("removeJob: " + formatKey(jobKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		DeleteItemRequest req = new DeleteItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true)
				.withValue(new AttributeValue(formatKey(jobKey))));
		try {
			synchronized (this.client) {
				this.client.deleteItem(req);
			}
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			return false;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		LOG.trace("removeJobs");
		// TODO: Use batch write
		boolean removed = true;
		for (JobKey k : jobKeys) {
			removed = removeJob(k) ? removed : false;
		}
		return removed;
	}

	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		LOG.trace("retrieveJob: " + formatKey(jobKey));
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
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.trace("storeTrigger");
		storeTrigger(newTrigger, replaceExisting, TriggerState.NORMAL);
	}

	@Override
	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.trace("removeTrigger: " + formatKey(triggerKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		DeleteItemRequest req = new DeleteItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(true)
				.withValue(new AttributeValue(formatKey(triggerKey))));
		try {
			this.client.deleteItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			return false;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		LOG.trace("removeTriggers");
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
		LOG.trace("replaceTrigger");
		OperableTrigger t = retrieveTrigger(triggerKey);
		if (t != null) {
			newTrigger.setJobKey(t.getJobKey());
			removeTrigger(triggerKey);
			storeTrigger(newTrigger, true);
		}
		return false;
	}

	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.trace("retrieveTrigger: " + formatKey(triggerKey));
		return retrieveTrigger(triggerKey, false);
	}

	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		LOG.trace("checkExists");
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
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return false;
	}

	@Override
	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.trace("checkExists");
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
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return false;
	}

	@Override
	public void clearAllSchedulingData() throws JobPersistenceException {
		LOG.trace("clearAllSchedulingData");
		clearTable(this.tableNameCalendars, KEY_NAME);
		clearTable(this.tableNameTriggers, KEY_KEY);
		clearTable(this.tableNameJobs, KEY_KEY);
	}

	@Override
	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.trace("storeCalendar");
		Map<String, AttributeValue> item = calendarToItem(calendar);
		item.put(KEY_NAME, new AttributeValue().withS(name));
		LOG.trace("  item: " + item.toString());
		PutItemRequest req = new PutItemRequest();
		req.withTableName(this.tableNameCalendars);
		req.withItem(item);
		if (!replaceExisting) {
			req.addExpectedEntry(KEY_NAME, new ExpectedAttributeValue(false));
		}
		try {
			LOG.trace("  putting key: " + item.get(KEY_NAME).getS());
			this.client.putItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			throw new ObjectAlreadyExistsException(name);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		if (updateTriggers) {
			List<OperableTrigger> tl = getTriggersForCalendar(name);
			for (OperableTrigger t : tl) {
				t.updateWithNewCalendar(calendar, this.misfireThreshold);
				storeTrigger(t, true);
			}
		}
	}

	@Override
	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		LOG.trace("removeCalendar: " + calName);
		List<OperableTrigger> tl = getTriggersForCalendar(calName);
		if (tl.size() > 0) {
			throw new JobPersistenceException("Triggers using calendar "
					+ calName + " exists.");
		}
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_NAME, new AttributeValue(calName));
		DeleteItemRequest req = new DeleteItemRequest();
		req.withTableName(this.tableNameCalendars);
		req.withKey(km);
		req.addExpectedEntry(KEY_NAME, new ExpectedAttributeValue(true)
				.withValue(new AttributeValue(calName)));
		try {
			this.client.deleteItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			return false;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		LOG.trace("retrieveCalendar");
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_NAME, new AttributeValue(calName));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameCalendars);
		req.withKey(km);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				return itemToCalendar(item);
			}
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		LOG.trace("getNumberOfJobs");
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		LOG.trace("getNumberOfTriggers");
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		LOG.trace("getNumberOfCalendars");
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.trace("getJobKeys: " + matcher.toString());
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
							&& strValue(item, KEY_GROUP) != null
							&& !strValue(item, KEY_GROUP).endsWith(val)) {
						continue;
					}
					keys.add(parseJobKey(strValue(item, KEY_KEY)));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return keys;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.trace("getTriggerKeys: " + matcher.toString());
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
							&& strValue(item, KEY_GROUP) != null
							&& !strValue(item, KEY_GROUP).endsWith(val)) {
						continue;
					}
					keys.add(parseTriggerKey(strValue(item, KEY_KEY)));
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return keys;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException {
		LOG.trace("getJobGroupNames");
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
				if (res != null) {
					List<Map<String, AttributeValue>> l = res.getItems();
					if (l != null) {
						for (Map<String, AttributeValue> item : l) {
							groups.add(strValue(item, KEY_GROUP));
						}
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		LOG.trace("getTriggerGroupNames");
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
				if (res != null) {
					List<Map<String, AttributeValue>> l = res.getItems();
					if (l != null) {
						for (Map<String, AttributeValue> item : l) {
							groups.add(strValue(item, KEY_GROUP));
						}
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<String> getCalendarNames() throws JobPersistenceException {
		LOG.trace("getCalendarNames");
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
				if (res != null) {
					List<Map<String, AttributeValue>> l = res.getItems();
					if (l != null) {
						for (Map<String, AttributeValue> item : l) {
							groups.add(strValue(item, KEY_NAME));
						}
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return new ArrayList<String>(groups);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		LOG.trace("getTriggersForJob: " + formatKey(jobKey));
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
				if (res != null) {
					List<Map<String, AttributeValue>> l = res.getItems();
					if (l != null) {
						for (Map<String, AttributeValue> item : l) {
							try {
								triggers.add(itemToTrigger(item));
							} catch (ClassNotFoundException e) {
								LOG.error(e.getMessage(), e);
							}
						}
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.trace("getTriggerState: " + formatKey(triggerKey));
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return TriggerState.NONE;
	}

	@Override
	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		LOG.trace("pauseTrigger: " + formatKey(triggerKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.PAUSED.name()),
				AttributeAction.PUT));
		req.addExpectedEntry(
				KEY_STATE,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue(TriggerState.NORMAL.name())));
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.trace("pauseTriggers: " + matcher.toString());
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
		LOG.trace("pauseJob: " + formatKey(jobKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.PAUSED.name()),
				AttributeAction.PUT));
		req.addExpectedEntry(KEY_STATE, new ExpectedAttributeValue()
				.withComparisonOperator(ComparisonOperator.NULL));
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		List<TriggerKey> tkl = getTriggerKeysForJob(jobKey);
		for (TriggerKey tk : tkl) {
			pauseTrigger(tk);
		}
	}

	@Override
	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		LOG.trace("pauseJobs: " + groupMatcher.toString());
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
		LOG.trace("resumeTrigger: " + formatKey(triggerKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.withReturnValues(ReturnValue.ALL_NEW);
		req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate(
				new AttributeValue(TriggerState.NORMAL.name()),
				AttributeAction.PUT));
		req.addExpectedEntry(
				KEY_STATE,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue(TriggerState.PAUSED.name())));
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			UpdateItemResult res = this.client.updateItem(req);
			if (res != null) {
				Map<String, AttributeValue> item = res.getAttributes();
				try {
					OperableTrigger t = itemToTrigger(item);
					if (t != null) {
						applyMisfire(t);
					}
				} catch (ClassNotFoundException e) {
					LOG.error(e.getMessage(), e);
				}
			}
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		LOG.trace("resumeTriggers: " + matcher.toString());
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
		LOG.trace("getPausedTriggerGroups");
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
				if (res != null) {
					List<Map<String, AttributeValue>> l = res.getItems();
					if (l != null) {
						for (Map<String, AttributeValue> item : l) {
							groups.add(strValue(item, KEY_GROUP));
						}
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return groups;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		LOG.trace("resumeJob: " + formatKey(jobKey));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(jobKey)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		req.addAttributeUpdatesEntry(KEY_STATE,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		req.addExpectedEntry(
				KEY_STATE,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.EQ).withAttributeValueList(
						new AttributeValue(TriggerState.PAUSED.name())));
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		List<TriggerKey> tkl = getTriggerKeysForJob(jobKey);
		for (TriggerKey tk : tkl) {
			resumeTrigger(tk);
		}
	}

	@Override
	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		LOG.trace("resumeJobs: " + matcher.toString());
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
		LOG.trace("pauseAll");
		Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.anyTriggerGroup());
		for (TriggerKey k : keys) {
			pauseTrigger(k);
		}
	}

	@Override
	public void resumeAll() throws JobPersistenceException {
		LOG.trace("resumeAll");
		Set<TriggerKey> keys = getTriggerKeys(GroupMatcher.anyTriggerGroup());
		for (TriggerKey k : keys) {
			resumeTrigger(k);
		}
	}

	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		LOG.trace("acquireNextTriggers: noLaterThan: " + noLaterThan
				+ " maxCount: " + maxCount + " timeWindow: " + timeWindow);
		LOG.trace("\n" + printTable(this.tableNameTriggers));
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.addScanFilterEntry(
				KEY_STATE,
				new Condition().withComparisonOperator(ComparisonOperator.EQ)
						.withAttributeValueList(
								new AttributeValue(TriggerState.NORMAL.name())));
		req.addScanFilterEntry(
				KEY_NEXT,
				new Condition().withComparisonOperator(ComparisonOperator.LE)
						.withAttributeValueList(
								new AttributeValue().withN(Long.toString(
										noLaterThan + timeWindow, 10))));
		req.addScanFilterEntry(KEY_LOCKED, new Condition()
				.withComparisonOperator(ComparisonOperator.NE)
				.withAttributeValueList(new AttributeValue().withBOOL(true)));
		List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
		try {
			boolean hasMore = true;
			ScanResult res = null;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
				List<Map<String, AttributeValue>> l = res.getItems();
				if (l != null) {
					for (Map<String, AttributeValue> item : l) {
						try {
							triggers.add(itemToTrigger(item));
						} catch (ClassNotFoundException e) {
							LOG.error(e.getMessage(), e);
						}
					}
				}
			}
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		List<OperableTrigger> acquired = new ArrayList<OperableTrigger>();
		for (OperableTrigger t : triggers) {
			LOG.debug("  acquiring target: " + t.toString());
			if (applyMisfire(t)) {
				LOG.debug("    misfired");
				if (t.getNextFireTime() == null) {
					LOG.debug("      no next");
					removeTrigger(t.getKey());
				} else {
					LOG.debug("      has next");
					// acquire(t.getKey());
					// acquired.add(t);
				}
			} else {
				LOG.debug("    not misfired");
				acquire(t.getKey());
				acquired.add(t);
			}
		}
		LOG.trace("\n" + printTable(this.tableNameTriggers));
		return acquired;
	}

	@Override
	public void releaseAcquiredTrigger(OperableTrigger trigger) {
		LOG.trace("releaseAcquiredTrigger: " + formatKey(trigger.getKey()));
		try {
			release(trigger.getKey());
		} catch (ObjectAlreadyExistsException e) {
			LOG.error(e.getMessage(), e);
		} catch (JobPersistenceException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
		LOG.trace("triggersFired");
		List<TriggerFiredResult> fired = new ArrayList<TriggerFiredResult>();
		for (OperableTrigger t : triggers) {
			if (t == null) {
				continue;
			}
			OperableTrigger t2 = retrieveTrigger(t.getKey(), true);
			if (t2 == null) {
				LOG.error("Trigger released or deleted during execution: "
						+ formatKey(t.getKey()));
				continue;
			}
			LOG.trace("  fired trigger: " + formatKey(t.getKey()));
			Calendar cal = null;
			if (t.getCalendarName() != null) {
				LOG.trace("  calendar: " + t.getCalendarName());
				cal = retrieveCalendar(t.getCalendarName());
				if (cal == null) {
					LOG.error("Calendar used for trigger is null: "
							+ formatKey(t.getKey()));
					continue;
				}
			}
			LOG.trace("before: " + t.toString());
			Date prev = t.getPreviousFireTime();
			t.triggered(cal);
			LOG.trace("after: " + t.toString());
			LOG.trace("  next: " + t.getNextFireTime());
			JobDetail j2 = retrieveJob(t.getJobKey());
			if (j2 == null) {
				continue;
			}
			TriggerFiredBundle bundle = new TriggerFiredBundle(
					retrieveJob(t.getJobKey()), t, cal, false, new Date(),
					t.getPreviousFireTime(), prev, t.getNextFireTime());
			JobDetail job = bundle.getJobDetail();
			if (job != null) {
				if (job.isConcurrentExectionDisallowed()) {
					LOG.trace("Trigger job is not concurrent: "
							+ formatKey(t.getJobKey()));
					List<TriggerKey> l = getTriggerKeysForJob(job.getKey());
					if (l != null) {
						for (TriggerKey tk : l) {
							acquire(tk);
						}
					}
					acquire(job.getKey());
				} else {
					LOG.trace("Trigger job is concurrent: "
							+ formatKey(t.getJobKey()));
					// release(t.getKey());
				}
			}
			if (t.getNextFireTime() != null) {
				LOG.trace("Trigger has next: " + formatKey(t.getKey()));
				storeTrigger(t, true);
				changeState(t.getKey(), TriggerState.NORMAL);
				release(t.getKey());
			} else {
				LOG.trace("Trigger has no next: " + formatKey(t.getKey()));
				storeTrigger(t, true);
				changeState(t.getKey(), TriggerState.COMPLETE);
				release(t.getKey());
			}
			fired.add(new TriggerFiredResult(bundle));
		}
		return fired;
	}

	@Override
	public void triggeredJobComplete(OperableTrigger trigger,
			JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		LOG.trace("triggeredJobComplete: triggerInstCode: " + triggerInstCode);

		// check for job deleted during execution
		JobDetail j = null;
		try {
			j = retrieveJob(trigger.getJobKey());
		} catch (JobPersistenceException e) {
		}
		if (j == null) {
			LOG.error("Job is deleted: " + formatKey(trigger.getJobKey()));
		} else {
			if (j.isPersistJobDataAfterExecution()) {
				LOG.trace("  persist job data");
				JobDataMap data = jobDetail.getJobDataMap();
				if (data != null) {
					data = (JobDataMap) data.clone();
					data.clearDirtyFlag();
				}
				try {
					updateData(trigger.getJobKey(), data);
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
			}
			if (j.isConcurrentExectionDisallowed()) {
				LOG.trace("  job not concurrent");
				try {
					List<TriggerKey> l = getTriggerKeysForJob(trigger
							.getJobKey());
					if (l != null) {
						for (TriggerKey tk : l) {
							release(tk);
						}
					}
					release(trigger.getJobKey());
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
				this.signaler.signalSchedulingChange(0L);
			}
		}

		// check for trigger deleted during execution
		OperableTrigger t = null;
		try {
			t = retrieveTrigger(trigger.getKey());
		} catch (JobPersistenceException e) {
		}
		if (t == null) {
			LOG.error("Trigger is deleted: " + formatKey(trigger.getKey()));
		} else {
			switch (triggerInstCode) {
			case NOOP:
				break;
			case DELETE_TRIGGER:
				if (trigger.getNextFireTime() == null) {
					LOG.trace("  trigger next is null");
					try {
						if (t.getNextFireTime() == null) {
							LOG.trace("  t next is null");
							removeTrigger(trigger.getKey());
						} else {
							LOG.trace("  t next is not null");
						}
					} catch (JobPersistenceException e) {
						LOG.error(e.getMessage(), e);
					}
				} else {
					LOG.trace("  trigger has next");
					try {
						removeTrigger(trigger.getKey());
						this.signaler.signalSchedulingChange(0L);
					} catch (JobPersistenceException e) {
						LOG.error(e.getMessage(), e);
					}
				}
				break;
			case SET_TRIGGER_COMPLETE:
				try {
					changeState(trigger.getKey(), TriggerState.COMPLETE);
				} catch (ObjectAlreadyExistsException e) {
					LOG.error(e.getMessage(), e);
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
				this.signaler.signalSchedulingChange(0L);
				break;
			case SET_TRIGGER_ERROR:
				LOG.error("Trigger " + trigger.getKey().toString()
						+ " state turned to ERROR");
				try {
					changeState(trigger.getKey(), TriggerState.ERROR);
				} catch (ObjectAlreadyExistsException e) {
					LOG.error(e.getMessage(), e);
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
				this.signaler.signalSchedulingChange(0L);
				break;
			case SET_ALL_JOB_TRIGGERS_COMPLETE:
				LOG.error("All triggers for " + trigger.getJobKey().toString()
						+ " state turned to COMPLETE");
				try {
					List<TriggerKey> tkl = getTriggerKeysForJob(trigger
							.getJobKey());
					for (TriggerKey tk : tkl) {
						changeState(tk, TriggerState.COMPLETE);
					}
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
				this.signaler.signalSchedulingChange(0L);
				break;
			case SET_ALL_JOB_TRIGGERS_ERROR:
				LOG.error("All triggers for " + trigger.getJobKey().toString()
						+ " state turned to ERROR");
				try {
					List<TriggerKey> tkl = getTriggerKeysForJob(trigger
							.getJobKey());
					for (TriggerKey tk : tkl) {
						changeState(tk, TriggerState.ERROR);
					}
				} catch (JobPersistenceException e) {
					LOG.error(e.getMessage(), e);
				}
				this.signaler.signalSchedulingChange(0L);
				break;
			default:
				break;
			}
		}
	}

	@Override
	public void setInstanceId(String instanceId) {
		LOG.debug("setInstanceId: " + instanceId);
		this.instanceId = instanceId;
	}

	@Override
	public void setInstanceName(String instanceName) {
		LOG.debug("setInstanceName: " + instanceName);
		this.instanceName = instanceName;
	}

	@Override
	public void setThreadPoolSize(int poolSize) {
		LOG.debug("setThreadPoolSize: " + poolSize);
		this.poolSize = poolSize;
	}

	public void setPrefix(String prefix) {
		LOG.debug("setPrefix: " + prefix);
		this.prefix = prefix;
		if (this.prefix != null && !this.prefix.isEmpty()) {
			this.tableNameCalendars = this.prefix + "_"
					+ this.tableNameCalendars;
			this.tableNameJobs = this.prefix + "_" + this.tableNameJobs;
			this.tableNameTriggers = this.prefix + "_" + this.tableNameTriggers;
		}
	}

	public void setRegion(String region) {
		LOG.debug("setRegion: " + region);
		this.region = Region.getRegion(Regions.fromName(region));
	}

	public void setEndpoint(String endpoint) {
		LOG.debug("setEndpoint: " + endpoint);
		this.endpoint = endpoint;
		if (this.endpoint != null && !this.endpoint.isEmpty()) {
			try {
				URL url = new URL(this.endpoint);
				if (url != null) {
					this.useEndpoint = true;
				}
			} catch (MalformedURLException e) {
				LOG.error(e.getMessage(), e);
				this.useEndpoint = false;
			}
		} else {
			this.useEndpoint = false;
		}
	}

	public void setMisfireThreshold(long misfireThreshold) {
		LOG.debug("setMisfireThreshold: " + misfireThreshold);
		this.misfireThreshold = misfireThreshold;
	}

	public void setClustered(boolean clustered) {
		LOG.debug("setClustered: " + clustered);
		this.clustered = clustered;
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
			LOG.warn("Creating table: " + this.tableNameCalendars);
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
			LOG.warn("Creating table: " + this.tableNameJobs);
			this.client.createTable(
					Arrays.asList(new AttributeDefinition().withAttributeName(
							KEY_KEY).withAttributeType(ScalarAttributeType.S)),
					this.tableNameJobs,
					Arrays.asList(new KeySchemaElement().withAttributeName(
							KEY_KEY).withKeyType(KeyType.HASH)),
					new ProvisionedThroughput(2L, 2L));
		}
		if (!Tables.doesTableExist(this.client, this.tableNameTriggers)) {
			LOG.warn("Creating table: " + this.tableNameTriggers);
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
			Tables.awaitTableToBecomeActive(this.client, this.tableNameJobs,
					60000, 1000);
			Tables.awaitTableToBecomeActive(this.client,
					this.tableNameTriggers, 60000, 1000);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			this.shutdown();
		}
	}

	/**
	 * Based on {@link org.quartz.simpl.RAMJobStore#applyMisfire}
	 *
	 * @param t
	 *            Trigger
	 * @return Misfired status
	 * @throws JobPersistenceException
	 */
	private boolean applyMisfire(OperableTrigger t)
			throws JobPersistenceException {
		long misfireTime = System.currentTimeMillis();
		if (this.misfireThreshold > 0) {
			misfireTime -= this.misfireThreshold;
		}
		Date next = t.getNextFireTime();
		if (next == null
				|| next.getTime() > misfireTime
				|| t.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
			LOG.trace("Trigger not misfired");
			return false;
		}
		Calendar cal = null;
		if (t.getCalendarName() != null) {
			cal = retrieveCalendar(t.getCalendarName());
		}
		this.signaler.notifyTriggerListenersMisfired((Trigger) t.clone());
		t.updateAfterMisfire(cal);
		if (next.equals(t.getNextFireTime())) {
			LOG.trace("Trigger not misfired (after updateAfterMisfire)");
			return false;
		} else if (t.getNextFireTime() == null) {
			LOG.trace("Trigger has no next (after updateAfterMisfire)");
			// changeState(t.getKey(), TriggerState.COMPLETE);
			// removeTrigger(t.getKey());
			this.signaler.notifySchedulerListenersFinalized(t);
		}
		return true;
	}

	private boolean acquire(JobKey key) throws JobPersistenceException {
		LOG.trace("acquire: job: " + formatKey(key));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.addAttributeUpdatesEntry(KEY_LOCKED,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue().withBOOL(true)));
		req.addAttributeUpdatesEntry(
				KEY_LOCKEDAT,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(
								new AttributeValue().withN(Long.toString(System
										.currentTimeMillis()))));
		req.addAttributeUpdatesEntry(KEY_LOCKEDBY,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue(this.instanceId)));
		req.addExpectedEntry(
				KEY_LOCKED,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.NE).withValue(
						new AttributeValue().withBOOL(true)));
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error("Job already locked: " + formatKey(key));
			return false;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private boolean acquire(TriggerKey key) throws JobPersistenceException {
		LOG.trace("acquire: trigger: " + formatKey(key));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.addAttributeUpdatesEntry(KEY_LOCKED,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue().withBOOL(true)));
		req.addAttributeUpdatesEntry(
				KEY_LOCKEDAT,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(
								new AttributeValue().withN(Long.toString(System
										.currentTimeMillis()))));
		req.addAttributeUpdatesEntry(KEY_LOCKEDBY,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue(this.instanceId)));
		req.addExpectedEntry(
				KEY_LOCKED,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.NE).withValue(
						new AttributeValue().withBOOL(true)));
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error("Trigger already locked: " + formatKey(key));
			return false;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private boolean release(JobKey key) throws JobPersistenceException {
		LOG.trace("release: job: " + formatKey(key));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		req.addAttributeUpdatesEntry(KEY_LOCKED,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue().withBOOL(false)));
		req.addAttributeUpdatesEntry(KEY_LOCKEDBY,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		req.addAttributeUpdatesEntry(KEY_LOCKEDAT,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		req.addExpectedEntry(
				KEY_LOCKED,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.NE).withValue(
						new AttributeValue().withBOOL(false)));
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error("Job already unlocked: " + formatKey(key));
			return true;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private boolean release(TriggerKey key) throws JobPersistenceException {
		LOG.trace("release: trigger: " + formatKey(key));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		req.addAttributeUpdatesEntry(KEY_LOCKED,
				new AttributeValueUpdate().withAction(AttributeAction.PUT)
						.withValue(new AttributeValue().withBOOL(false)));
		req.addAttributeUpdatesEntry(KEY_LOCKEDBY,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		req.addAttributeUpdatesEntry(KEY_LOCKEDAT,
				new AttributeValueUpdate().withAction(AttributeAction.DELETE));
		req.addExpectedEntry(
				KEY_LOCKED,
				new ExpectedAttributeValue().withComparisonOperator(
						ComparisonOperator.NE).withValue(
						new AttributeValue().withBOOL(false)));
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			this.client.updateItem(req);
			return true;
		} catch (ConditionalCheckFailedException e) {
			LOG.error("Trigger already unlocked: " + formatKey(key));
			return true;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private TriggerState changeState(TriggerKey key, TriggerState state)
			throws JobPersistenceException {
		LOG.trace("changeState: trigger: " + formatKey(key) + " state: "
				+ state);
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		if (state != null) {
			req.addAttributeUpdatesEntry(KEY_STATE,
					new AttributeValueUpdate().withAction(AttributeAction.PUT)
							.withValue(new AttributeValue(state.name())));
			req.addExpectedEntry(
					KEY_LOCKED,
					new ExpectedAttributeValue().withComparisonOperator(
							ComparisonOperator.NE).withValue(
							new AttributeValue(state.name())));
		} else {
			req.addAttributeUpdatesEntry(KEY_STATE, new AttributeValueUpdate()
					.withAction(AttributeAction.DELETE));
			req.addExpectedEntry(KEY_LOCKED, new ExpectedAttributeValue()
					.withComparisonOperator(ComparisonOperator.NULL));
		}
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			UpdateItemResult res = this.client.updateItem(req);
			if (res != null && res.getAttributes() != null
					&& res.getAttributes().containsKey(KEY_STATE)) {
				String s = strValue(res.getAttributes(), KEY_STATE);
				return TriggerState.valueOf(s);
			}
		} catch (ConditionalCheckFailedException e) {
			LOG.error("Trigger state is the same: " + formatKey(key)
					+ " state: " + state);
			return state;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	@SuppressWarnings("deprecation")
	private JobDataMap updateData(JobKey key, JobDataMap data)
			throws JobPersistenceException {
		LOG.trace("updateData: job: " + formatKey(key));
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(key)));
		UpdateItemRequest req = new UpdateItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withKey(km);
		if (data != null) {
			data.clearDirtyFlag();
			data.removeTransientData();
			req.addAttributeUpdatesEntry(
					KEY_DATA,
					new AttributeValueUpdate().withAction(AttributeAction.PUT)
							.withValue(
									new AttributeValue().withM(mapToItem(data
											.getWrappedMap()))));
		} else {
			req.addAttributeUpdatesEntry(KEY_DATA, new AttributeValueUpdate()
					.withAction(AttributeAction.DELETE));
		}
		LOG.trace("  entry: " + req.getAttributeUpdates());
		req.withReturnValues(ReturnValue.UPDATED_OLD);
		try {
			LOG.trace("  updating key: " + km.get(KEY_KEY).getS());
			UpdateItemResult res = this.client.updateItem(req);
			if (res != null && res.getAttributes() != null
					&& res.getAttributes().containsKey(KEY_DATA)) {
				Map<String, Object> m = mapValue(res.getAttributes(), KEY_DATA);
				return new JobDataMap(m);
			}
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	private void storeJob(JobDetail newJob, boolean replaceExisting,
			TriggerState state) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		LOG.trace("storeJob: " + formatKey(newJob.getKey()) + " replace: "
				+ replaceExisting + " state: " + state);
		Map<String, AttributeValue> item = jobToItem(newJob);
		if (state != null) {
			attr(item, KEY_STATE, state.name());
		}
		LOG.trace("  item: " + item.toString());
		PutItemRequest req = new PutItemRequest();
		req.withTableName(this.tableNameJobs);
		req.withItem(item);
		if (!replaceExisting) {
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(false));
		}
		try {
			LOG.trace("  putting key: " + item.get(KEY_KEY).getS());
			this.client.putItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			throw new ObjectAlreadyExistsException(newJob);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private void storeTrigger(OperableTrigger newTrigger,
			boolean replaceExisting, TriggerState state)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		LOG.trace("storeTrigger: " + formatKey(newTrigger.getKey())
				+ " replace: " + replaceExisting + " state: " + state);
		Map<String, AttributeValue> item = triggerToItem(newTrigger);
		if (state != null) {
			attr(item, KEY_STATE, state.name());
		}
		LOG.trace("  item: " + item.toString());
		PutItemRequest req = new PutItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withItem(item);
		if (!replaceExisting) {
			req.addExpectedEntry(KEY_KEY, new ExpectedAttributeValue(false));
		}
		try {
			LOG.trace("  putting key: " + item.get(KEY_KEY).getS());
			this.client.putItem(req);
		} catch (ConditionalCheckFailedException e) {
			LOG.error(e.getMessage(), e);
			throw new ObjectAlreadyExistsException(newTrigger);
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private OperableTrigger retrieveTrigger(TriggerKey triggerKey,
			boolean onlyMine) throws JobPersistenceException {
		LOG.trace("retrieveTrigger: " + formatKey(triggerKey) + " onlyMine: "
				+ onlyMine);
		Map<String, AttributeValue> km = new HashMap<String, AttributeValue>();
		km.put(KEY_KEY, new AttributeValue(formatKey(triggerKey)));
		GetItemRequest req = new GetItemRequest();
		req.withTableName(this.tableNameTriggers);
		req.withKey(km);
		try {
			GetItemResult res = this.client.getItem(req);
			Map<String, AttributeValue> item = res.getItem();
			if (item != null && !item.isEmpty()) {
				if (onlyMine) {
					if (boolValue(item, KEY_LOCKED)
							&& this.instanceId.equals(strValue(item,
									KEY_LOCKEDBY))) {
						return itemToTrigger(item);
					} else {
						return null;
					}
				} else {
					return itemToTrigger(item);
				}
			}
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
		return null;
	}

	private List<TriggerKey> getTriggerKeysForJob(JobKey jobKey)
			throws JobPersistenceException {
		LOG.trace("getTriggersForJob");
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
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	private List<OperableTrigger> getTriggersForCalendar(String name)
			throws JobPersistenceException {
		LOG.trace("getTriggersForCalendar: " + name);
		ScanRequest req = new ScanRequest();
		req.withTableName(this.tableNameTriggers);
		req.addScanFilterEntry(KEY_CALENDAR, new Condition()
				.withComparisonOperator(ComparisonOperator.EQ)
				.withAttributeValueList(new AttributeValue(name)));
		try {
			boolean hasMore = true;
			ScanResult res = null;
			List<OperableTrigger> tl = new ArrayList<OperableTrigger>();
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				List<Map<String, AttributeValue>> l = res.getItems();
				if (l != null) {
					for (Map<String, AttributeValue> item : l) {
						try {
							tl.add(itemToTrigger(item));
						} catch (ClassNotFoundException e) {
							LOG.error(e.getMessage(), e);
						}
					}
				}
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
			return tl;
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
			throw new JobPersistenceException(e.getMessage(), e);
		}
	}

	@SuppressWarnings("deprecation")
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
		if (j.getJobDataMap() != null) {
			j.getJobDataMap().clearDirtyFlag();
			j.getJobDataMap().removeTransientData();
			attr(item, KEY_DATA, j.getJobDataMap().getWrappedMap());
		}
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
		attr(item, KEY_END, t.getEndTime());
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
			attr(item, KEY_BYTECODE, serialize(t));
		}
		return item;
	}

	private Map<String, AttributeValue> calendarToItem(Calendar c) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		attr(item, KEY_CLASS, c.getClass().getName());
		attr(item, KEY_DESCRIPTION, c.getDescription());
		if (c.getBaseCalendar() != null) {
			attr(item, KEY_BASE, serialize(c.getBaseCalendar()));
		}
		attr(item, KEY_DATA, serialize(c));
		return item;
	}

	private synchronized void clearTable(String name, String... keys) {
		LOG.trace("clearTable: " + name);
		List<Map<String, AttributeValue>> allKeys = new ArrayList<Map<String, AttributeValue>>();
		ScanRequest req = new ScanRequest();
		req.withTableName(name);
		req.withAttributesToGet(keys);
		try {
			boolean hasMore = true;
			ScanResult res = null;
			while (hasMore) {
				hasMore = false;
				res = this.client.scan(req);
				allKeys.addAll(res.getItems());
				Map<String, AttributeValue> lastKey = res.getLastEvaluatedKey();
				if (lastKey != null && !lastKey.isEmpty()) {
					hasMore = true;
					req.withExclusiveStartKey(lastKey);
				}
			}
		} catch (AmazonServiceException e) {
			LOG.error(e.getMessage(), e);
		} catch (AmazonClientException e) {
			LOG.error(e.getMessage(), e);
		}
		deleteItems(name, allKeys);
	}

	private synchronized void deleteItems(String name,
			List<Map<String, AttributeValue>> keys) {
		Queue<Map<String, AttributeValue>> queue = new LinkedList<Map<String, AttributeValue>>(
				keys);
		while (!queue.isEmpty()) {
			Map<String, AttributeValue> k = queue.poll();
			LOG.trace("deleting " + k.toString() + " from " + name);
			DeleteItemRequest req = new DeleteItemRequest();
			req.withTableName(name);
			req.withKey(k);
			try {
				this.client.deleteItem(req);
			} catch (AmazonServiceException e) {
				LOG.error(e.getMessage(), e);
			} catch (AmazonClientException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	@SuppressWarnings("unused")
	private synchronized void batchDeleteItems(String name,
			List<Map<String, AttributeValue>> keys) {
		Queue<Map<String, AttributeValue>> queue = new LinkedList<Map<String, AttributeValue>>(
				keys);
		while (!queue.isEmpty()) {
			List<WriteRequest> l = new ArrayList<WriteRequest>(
					DYNAMODB_MAXBATCHWRITE);
			for (int i = 0; !queue.isEmpty() && i < DYNAMODB_MAXBATCHWRITE; i++) {
				Map<String, AttributeValue> k = queue.poll();
				LOG.trace("deleting " + k.toString() + " from " + name);
				DeleteRequest dr = new DeleteRequest();
				dr.withKey(k);
				WriteRequest wr = new WriteRequest();
				wr.withDeleteRequest(dr);
				l.add(wr);
			}
			Map<String, List<WriteRequest>> reqs = new HashMap<String, List<WriteRequest>>();
			reqs.put(name, l);
			try {
				BatchWriteItemResult res = this.client.batchWriteItem(reqs);
				Map<String, List<WriteRequest>> u = res.getUnprocessedItems();
				if (u != null && !u.isEmpty() && u.containsKey(name)) {
					List<WriteRequest> ul = u.get(name);
					for (WriteRequest wr : ul) {
						DeleteRequest dr = wr.getDeleteRequest();
						if (dr != null && dr.getKey() != null) {
							queue.add(dr.getKey());
						}
					}
				}
			} catch (AmazonServiceException e) {
				LOG.error(e.getMessage(), e);
			} catch (AmazonClientException e) {
				LOG.error(e.getMessage(), e);
			}
		}
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
		JobKey key = parseJobKey(strValue(item, KEY_KEY));
		@SuppressWarnings("unchecked")
		Class<Job> cls = (Class<Job>) this.loadHelper.getClassLoader()
				.loadClass(strValue(item, KEY_CLASS));
		JobBuilder builder = JobBuilder.newJob(cls)
				.withIdentity(key.getName(), key.getGroup())
				.withDescription(strValue(item, KEY_DESCRIPTION))
				.storeDurably(boolValue(item, KEY_DURABLE));
		Map<String, Object> m = mapValue(item, KEY_DATA);
		if (m != null && !m.isEmpty()) {
			builder.usingJobData(new JobDataMap(m));
		}
		return builder.build();
	}

	private OperableTrigger itemToTrigger(Map<String, AttributeValue> item)
			throws ClassNotFoundException {
		OperableTrigger t = null;
		String type = strValue(item, KEY_TYPE);
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
						LOG.error(e.getMessage(), e);
					}
				}
				String tz = strValue(item, KEY_TIMEZONE);
				if (tz != null && !tz.isEmpty()) {
					((CronTriggerImpl) t).setTimeZone(TimeZone.getTimeZone(tz));
				}
			} else {
				t = (OperableTrigger) deserialize(strValue(item, KEY_BYTECODE));
			}
		}
		if (t != null && !TRIGGERTYPE_UNKNOWN.equalsIgnoreCase(type)) {
			t.setKey(parseTriggerKey(strValue(item, KEY_KEY)));
			t.setJobKey(parseJobKey(strValue(item, KEY_JOB)));
			t.setDescription(strValue(item, KEY_DESCRIPTION));
			t.setFireInstanceId(strValue(item, KEY_INSTANCE));
			t.setCalendarName(strValue(item, KEY_CALENDAR));
			t.setPriority(intValue(item, KEY_PRIORITY));
			t.setMisfireInstruction(intValue(item, KEY_MISFIRE));
			t.setStartTime(dateValue(item, KEY_START));
			t.setEndTime(dateValue(item, KEY_END));
			t.setNextFireTime(dateValue(item, KEY_NEXT));
			t.setPreviousFireTime(dateValue(item, KEY_PREV));
		}
		return t;
	}

	private Calendar itemToCalendar(Map<String, AttributeValue> item)
			throws ClassNotFoundException {
		String s = strValue(item, KEY_DATA);
		return (Calendar) deserialize(s);
	}

	private static String strValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			return value.getS();
		}
		return null;
	}

	private static Boolean boolValue(Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			return value.getBOOL();
		}
		return false;
	}

	private static Map<String, Object> mapValue(
			Map<String, AttributeValue> map, String key) {
		AttributeValue value = map.get(key);
		if (value != null) {
			Map<String, AttributeValue> m = value.getM();
			if (m != null) {
				return itemToMap(m);
			}
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
			Map<String, Object> value) {
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

	private static String serialize(Object o) {
		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;
		try {
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(o);
			oos.flush();
			baos.flush();
			return DatatypeConverter.printBase64Binary(baos.toByteArray());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
				}
			}
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}

	private static Object deserialize(String s) {
		ByteArrayInputStream bais = null;
		ObjectInputStream ois = null;
		try {
			byte[] b = DatatypeConverter.parseBase64Binary(s);
			bais = new ByteArrayInputStream(b);
			ois = new ObjectInputStream(bais);
			return ois.readObject();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
				}
			}
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}

	// for debugging only
	private String printTable(String tbl) {
		StringBuffer sb = new StringBuffer();
		Map<String, AttributeValue> last = null;
		boolean hasMore = true;
		ScanRequest req = new ScanRequest().withTableName(tbl);
		while (hasMore) {
			hasMore = false;
			if (last != null) {
				req.withExclusiveStartKey(last);
			}
			ScanResult res = this.client.scan(req);
			req.clearExclusiveStartKeyEntries();
			if (res != null) {
				last = res.getLastEvaluatedKey();
				if (last != null && !last.isEmpty()) {
					hasMore = true;
				}
				List<Map<String, AttributeValue>> l = res.getItems();
				if (l != null) {
					for (Map<String, AttributeValue> m : l) {
						sb.append(m.toString());
						sb.append("\r\n");
					}
				}
			}
		}
		return sb.toString();
	}
}
