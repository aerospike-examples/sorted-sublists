package com.aerospike.storage.subkeys;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class TestDistributedLockManager {
	private static final String LOCK_BIN = "lock";
	
	private static ExecutorService executorService = Executors.newFixedThreadPool(50);
	private static enum LogLevel {DEBUG, INFO};
	
	private static final LogLevel logLevel = LogLevel.DEBUG;
	private static final long startTime = System.currentTimeMillis();

	private void submitTransaction(int transactionAmount, Key testKey, DistributedLockManager lockManager, IAerospikeClient client, Random random) throws InterruptedException {
		while (true) {
			Record record = null;
			long initialAcquireTime = System.currentTimeMillis();
			try {
				record = lockManager.acquireLock(testKey, 100, "exposure", "limit");
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.TIMEOUT) {
					// This thread was starved getting the lock.
					long now = System.currentTimeMillis();
					System.out.printf("[%d, %d] ...Took too long to acquire the lock, began waiting at %d and it's now %d. Going to retry...\n", Thread.currentThread().getId(), now-startTime, initialAcquireTime-startTime, now-startTime);
				}
				else {
					throw ae;
				}
			}
			
			if (record == null) {
				WritePolicy wp = new WritePolicy();
				wp.recordExistsAction = RecordExistsAction.CREATE_ONLY;
				try {
					client.put(wp, testKey, new Bin("exposure", transactionAmount), new Bin("limit", 100000));
					break;
				}
				catch (AerospikeException ae) {
					if (ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
						if (logLevel == LogLevel.DEBUG) {
							System.out.printf("[%d, %d]: Tried to create record but it already existed\n", Thread.currentThread().getId(), System.currentTimeMillis() - startTime);
							// fall through to retry
						}
					}
					else {
						throw ae;
					}
				}
			}
			else {
				if (logLevel == LogLevel.DEBUG) {
					System.out.printf("[%d, %d]: Acquired lock\n", Thread.currentThread().getId(), System.currentTimeMillis() - startTime);
				}
				long exposure = record.getLong("exposure");
				
				Thread.sleep(random.nextInt(6)+1);
				// Allows this transaction to proceed
				if (logLevel == LogLevel.DEBUG) {
					System.out.printf("[%d, %d]: Releasing lock and updating exising balance %d to %d\n", Thread.currentThread().getId(), System.currentTimeMillis() - startTime, exposure, (exposure+transactionAmount));
				}
				lockManager.updateRecordAndReleaseLock(null, testKey, record.generation, Operation.put(new Bin("exposure", exposure + transactionAmount)));
				break;
			}
		}
	}
	
	@Test
	public void testDistributedLockManager() throws InterruptedException {
		IAerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		
		DistributedLockManager lockManager = new DistributedLockManager(client, LOCK_BIN, 100);
		
		final Key testKey = new Key("test", "testSet", "123");
		client.delete(null, testKey);
		
		AtomicLong runningTotal = new AtomicLong(0); 
		for (int i = 0; i < 20; i++) {
			Thread.sleep(5);
			executorService.submit(() -> {
				Random random = new Random();
				int limit = random.nextInt(2000)+5;
				System.out.printf("[%d] Writing %d items\n", Thread.currentThread().getId(), limit);
				for (int j = 0; j < limit; j++) {
					try {
						int thisTxnAmt = random.nextInt(10000);
						submitTransaction(thisTxnAmt, testKey, lockManager, client, random);
						runningTotal.addAndGet(thisTxnAmt);
						Thread.sleep(7);
					}
					catch (AerospikeException ae) {
						System.err.printf("[%d, %d] Error: %s of type %s\n", Thread.currentThread().getId(), System.currentTimeMillis()-startTime, ae.getMessage(), ae.getClass());
						ae.printStackTrace();
						break;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
		}
		executorService.shutdown();
		executorService.awaitTermination(1, TimeUnit.HOURS);
		// Now read the current value and compare it to the expected value
		long value = client.get(null, testKey).getLong("exposure");
		assertEquals(runningTotal.get(), value);
		client.close();
	}
}
