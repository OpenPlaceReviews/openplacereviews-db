package org.openplacereviews.osm.service;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_TYPE;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.PerformanceMetrics;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class GenericMultiThreadBot<T> implements IOpenDBBot<T> {

	private static final Log LOGGER = LogFactory.getLog(GenericMultiThreadBot.class);
	private static final long TIMEOUT_OVERPASS_HOURS = 4;
	public static final String F_BLOCKCHAIN_CONFIG = "blockchain-config";
	public static final String F_PLACES_PER_OPERATION = "places_per_operation";
	public static final String F_OPERATIONS_PER_BLOCK = "operations_per_block";
	public static final String F_OPERATIONS_MIN_BLOCK_CAPACITY = "min_block_capacity";
	public static final String F_THREADS = "threads";
	private static final PerformanceMetric mBlock = PerformanceMetrics.i().getMetric("opr.osm-sync.block");
	
	private List<TaskResult> successfulResults = new ArrayList<>();
	protected long placesPerOperation = 250;
	protected long operationsPerBlock = 16;
	protected double blockCapacity = 0.8;
	protected OpObject botObject;
	protected String opType;
	
	@Autowired
	private BlocksManager blocksManager;
	
	@Autowired
	private LogOperationService logSystem;

	private ThreadPoolExecutor service;

	public GenericMultiThreadBot(OpObject botObject) {
		this.botObject = botObject;
	}
	
	protected static class TaskResult { 
		public TaskResult(String msg, Exception e) {
			this.msg = msg;
			this.e = e;
		}
		
		public TaskResult(String msg, int cnt, Exception e) {
			this.msg = msg;
			this.e = e;
			this.counter = cnt;
		}
		
		Exception e;
		String msg;
		int counter;
	}
	
	protected Future<TaskResult> submitTask(String msg, Callable<TaskResult> task, Deque<Future<TaskResult>> futures) {
		if(service == null) {
			return null;
		}
		Future<TaskResult> f = service.submit(task);
		futures.add(f);
		return f;
	}
	
	protected int submitTaskAndWait(String msg, Callable<TaskResult> task, Deque<Future<TaskResult>> futures)
			throws Exception {
		if(service == null) {
			return 0;
		}
		if(msg != null) {
			LOGGER.info(msg);
		}
		Future<TaskResult> f = service.submit(task);
		int cnt = 0;
		int processed = 0;
		TaskResult r = waitFuture(cnt++, processed, f, futures);
		processed += r.counter;
		while (!futures.isEmpty()) {
			if(isInterrupted()) {
				break;
			}
			r = waitFuture(cnt++, processed, futures.pop(), futures);
			processed += r.counter;
		}
		return processed;
	}
	

	private TaskResult waitFuture(int id, int overall,  Future<TaskResult> f, 
			Deque<Future<TaskResult>> futures) throws Exception {
		TaskResult r = f.get(TIMEOUT_OVERPASS_HOURS, TimeUnit.HOURS);
		int tot = id + futures.size();
		String msg = String.format("%d / %d (%d + %d): %s", id, tot, overall, r.counter, r.msg);
		if(r.e != null) {
			LOGGER.error(msg);
			throw r.e;
		} else {
			successfulResults.add(r);
			LOGGER.info(msg);
		}
		if (blocksManager.getBlockchain().getQueueOperations().size() >= operationsPerBlock && 
				blocksManager.getQueueCapacity() >= blockCapacity) {
			Metric m = mBlock.start();
			blocksManager.createBlock(blockCapacity);
			m.capture();
		}
		return r;
		
	}
	
	public OpOperation initOpOperation(String opType) {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(opType);
		opOperation.setSignedBy(blocksManager.getServerUser());
		return opOperation;
	}
	
	public OpOperation generateHashAndSignAndAdd(OpOperation opOperation) throws FailedVerificationException {
		JsonFormatter formatter = blocksManager.getBlockchain().getRules().getFormatter();
		opOperation = formatter.parseOperation(formatter.opToJson(opOperation));
		OpOperation op = blocksManager.generateHashAndSign(opOperation, blocksManager.getServerLoginKeyPair());
		blocksManager.addOperation(op);
		return op;
	}

	public boolean isInterrupted() {
		return this.service == null;
	}
	
	public List<TaskResult> getSuccessfulResults() {
		return successfulResults;
	}
	
	@Override
	public abstract String getTaskDescription();

	@Override
	public abstract String getTaskName();
	
	@Override
	public int taskCount() {
		return 1;
	}

	@Override
	public int total() {
		return 1 + (service  == null ? 1 : (int) service.getTaskCount());
	}

	@Override
	public int progress() {
		return 1 + (service == null ? 1 : (int) service.getCompletedTaskCount());
	}
	
	public TaskResult errorResult(String msg, Exception e) {
		logSystem.logError(botObject, ErrorType.BOT_PROCESSING_ERROR, getTaskName() + " failed: " + e.getMessage(), e);
		return new TaskResult(e.getMessage(), e);
	}
	
	public void logError(String msg) {
		logSystem.logError(botObject, ErrorType.BOT_PROCESSING_ERROR, msg, null);
	}
	
	protected void initVars() {
		OpObject nbot = blocksManager.getBlockchain().getObjectByName(botObject.getParentType(), botObject.getId());
		if(nbot == null) {
			throw new IllegalStateException("Can't retrieve bot object state");
		}
		botObject = nbot;
		this.opType = botObject.getStringMap(F_BLOCKCHAIN_CONFIG).get(F_TYPE);
		this.placesPerOperation = botObject.getField(placesPerOperation, F_BLOCKCHAIN_CONFIG,
				F_PLACES_PER_OPERATION);
		this.operationsPerBlock = botObject.getField(operationsPerBlock, F_BLOCKCHAIN_CONFIG,
				F_OPERATIONS_PER_BLOCK);
		this.blockCapacity = botObject.getField(blockCapacity, F_BLOCKCHAIN_CONFIG,
				F_OPERATIONS_MIN_BLOCK_CAPACITY);
		ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
				.setNameFormat(Thread.currentThread().getName() + "-%d").build();
		this.service = (ThreadPoolExecutor) Executors.newFixedThreadPool(botObject.getIntValue(F_THREADS, 1),
				namedThreadFactory);
		successfulResults.clear();
	}
	
	protected void shutdown() {
		if (this.service != null) {
			this.service.shutdownNow();
			this.service = null;
		}
	}


	@Override
	public boolean interrupt() {
		if(this.service != null) {
			this.service.shutdownNow();
			this.service = null;
			return true;
		}
		return false;
	}


}
