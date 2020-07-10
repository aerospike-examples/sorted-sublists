package com.aerospike.storage.subkeys;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

public class DistributedLockManager {
	
	public static class LockAcquireException extends AerospikeException {
		private static final long serialVersionUID = -8391011613878160767L;

		public LockAcquireException(Exception e) {
			super(e);
		}
	}

	public static class RecordDoesNotExistException extends LockAcquireException {
		private static final long serialVersionUID = 5442513337525060885L;

		public RecordDoesNotExistException(Exception e) {
			super(e);
		}
	}
	
	public static class RootBlockRemoved extends AerospikeException {
		private static final long serialVersionUID = 4712602585758167233L;

		public RootBlockRemoved(String message) {
			super(message);
		}
	}
	

	private final String lockBinName;
	private final IAerospikeClient client;
	private final long maxLockTime;
	private final int lockRetryTimeMs;
	
	public DistributedLockManager(IAerospikeClient client, String binNameToUseForLock, long maxLockTime) {
		this(client, binNameToUseForLock, maxLockTime, 1);
	}

	public DistributedLockManager(IAerospikeClient client, String binNameToUseForLock, long maxLockTime, int lockRetryTimeMs) {
		this.client = client;
		this.lockBinName = binNameToUseForLock;
		this.maxLockTime = maxLockTime;
		this.lockRetryTimeMs = lockRetryTimeMs;
	}

	/**
	 * A moderately unique ID 
	 */
	private static final String ID = UUID.randomUUID().toString(); // This needs to be unique only for this session, used only for locking

	private String getLockId() {
		return ID + "-" + Thread.currentThread().getId();
	}
	
	private void wait(int delayTime) {
		try {
			Thread.sleep(delayTime);
		} catch (InterruptedException e) {
			throw new AerospikeException(ResultCode.TIMEOUT, false);
		}
	}

	private Operation getObtainLockOperation(String id, long now) {
		if (id == null) {
			id = getLockId();
		}
		List<Object> data = Arrays.asList(new Object[] { id, now + maxLockTime });
		MapPolicy policy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);
		return MapOperation.put(policy, lockBinName, Value.get("locked"), Value.get(data));
	}
	
	private Operation getReleaseLockOperation(String id) {
		if (id == null) {
			id = getLockId();
		}

		List<Object> startData = Arrays.asList(new Object[] { id, Long.MIN_VALUE });
		List<Object> endData = Arrays.asList(new Object[] { id, Long.MAX_VALUE });

		return MapOperation.removeByValueRange(this.lockBinName, Value.get(startData), Value.get(endData), MapReturnType.RANK);
	}
	
	public Record performOperationsUnderLock(WritePolicy policy, Key key, Operation ... operations) throws AerospikeException {
		String id = getLockId();
		long now = new Date().getTime();
		Operation[] operationList = new Operation[operations.length+2];
		for (int i = 0; i < operationList.length; i++) {
			if (i == 0) {
				operationList[i] = getObtainLockOperation(id, now);
			}
			else if (i == operationList.length-1) {
				operationList[i] = getReleaseLockOperation(id);
			}
			else {
				operationList[i] = operations[i-1];
			}
		}
		long deadline = 0;
		int socketTimeout = policy.socketTimeout;
		int totalTimeout = policy.totalTimeout;

		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

			if (socketTimeout == 0 || socketTimeout > totalTimeout) {
				socketTimeout = totalTimeout;
			}
		}
		return this.performOperationsUnderLock(policy, key, socketTimeout, totalTimeout, deadline, 1, operations);
	}
	
	/**
	 * Perform the passed operation under a record level lock. If the lock is not held, the passed operations will be executed on the record associated with
	 * the passed key.
	 * <p/>
	 * If the record is locked, the operations will fail. What happens then depends on the settings in the passed WritePolicy. If retries have been defined
	 * the operation will be tried as per the WritePolicy settings
	 * @param policy
	 * @param key
	 * @param socketTimeout
	 * @param totalTimeout
	 * @param deadline
	 * @param iteration
	 * @param operationList
	 * @return
	 */
	private Record performOperationsUnderLock(WritePolicy policy, Key key, int socketTimeout, int totalTimeout, long deadline, int iteration, Operation[] operationList) {
		while (true) {
			try {
				return client.operate(policy, key, operationList);
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.ELEMENT_EXISTS) {
					// The lock is currently held by someone else, let's try re-acquiring if needed
					// Check maxRetries.
					if (iteration > policy.maxRetries) {
						// We cannot retry any more, just throw the exception
						throw ae;
					}

					if (policy.totalTimeout > 0) {
						// Check for total timeout.
						long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

						if (remaining <= 0) {
							break;
						}

						// Convert back to milliseconds for remaining check.
						remaining = TimeUnit.NANOSECONDS.toMillis(remaining);

						if (remaining < totalTimeout) {
							totalTimeout = (int)remaining;

							if (socketTimeout > totalTimeout) {
								socketTimeout = totalTimeout;
							}
						}
					}

					if (policy.sleepBetweenRetries > 0) {
						// Sleep before trying again.
						Util.sleep(policy.sleepBetweenRetries);
					}

					iteration++;
				}
				else {
					throw ae;
				}
			}
		}
		return null;
	}

	/**
	 * Acquire a lock. This method can throw Aerospike exceptions. If the lock is not acquired in timeoutInMs
	 * an exception will be thrown with a ReasonCode ot TIMEOUT. This waiting is a busy wait (polling) with a 
	 * retry time of LockRetyrTimeMs. (1ms by default). 
	 * @param key
	 * @param timeoutInMs
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Record acquireLock(Key key, long timeoutInMs, String ...bins) {
		long now = new Date().getTime();
		long timeoutEpoch = now + timeoutInMs;
		String id = this.getLockId();
		
		while (true) {
			try {
				List<Object> data = new ArrayList<>();
				data.add(id);
				data.add(now + this.maxLockTime);
				WritePolicy wp = new WritePolicy();
				wp.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
				if (bins != null) {
					Operation[] ops = new Operation[bins.length+1];
					ops[0] = getObtainLockOperation(id, now);
					for (int i = 0; i < bins.length; i++) {
						ops[i+1] = Operation.get(bins[i]);
					}
					Record record = client.operate(wp, key, ops);
					return record;
				}
				else {
					client.operate(wp, key, getObtainLockOperation(id, now));
					return null;
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
					// TODO: Should this be an error or just return null;
					//throw new RecordDoesNotExistException(ae);
					return null;
				}
				else if (ae.getResultCode() == ResultCode.ELEMENT_EXISTS) {
					// the lock is already owned
					Record record;
					if (bins != null) {
						record = client.get(null, key);
					}
					else {
						record = client.get(null, key, this.lockBinName);
					}
					if (record == null) {
						throw new RecordDoesNotExistException(ae);
					}
					else {
						Map<String, List<Object>> lockData = (Map<String, List<Object>>) record.getMap(this.lockBinName);
						if (lockData == null || lockData.isEmpty()) {
							// The lock seems to be released, continue to re-acquire
							continue;
						}
						else {
							List<Object> lockInfo = lockData.get("locked");
							String lockOwner = (String) lockInfo.get(0);
							long lockExpiry = (long) lockInfo.get(1);
							if (id.equals(lockOwner)) {
								// This thread already owns the lock, done!
								return record;
							}
							else {
								now = new Date().getTime();
								if (now < lockExpiry) {
									if (timeoutInMs > 0 && now >= timeoutEpoch) {
										// It's taken too long to get the lock
										throw new AerospikeException(ResultCode.TIMEOUT, false);
									}
									else {
										wait(this.lockRetryTimeMs);
									}
								}
								else {
									// The lock has expired. Try to force reacquiring the lock with a gen check
									WritePolicy writePolicy = new WritePolicy();
									writePolicy.generation = record.generation;
									writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
									
									List<Object> data = new ArrayList<>();
									data.add(id);
									data.add(now + this.maxLockTime);
									MapPolicy forcePolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
									try {
										if (bins != null) {
											Operation[] ops = new Operation[bins.length+1];
											ops[0] = MapOperation.put(forcePolicy, this.lockBinName, Value.get("locked"), Value.get(data));
											for (int i = 0; i < bins.length; i++) {
												ops[i+1] = Operation.get(bins[i]);
											}

											record = client.operate(writePolicy, key, ops);
											return record;
										}
										else {
											client.operate(writePolicy, key, MapOperation.put(forcePolicy, this.lockBinName, Value.get("locked"), Value.get(data)));
											return null;
										}
									}
									catch (AerospikeException ae2) {
										if (ae2.getResultCode() == ResultCode.GENERATION_ERROR) {
											// A different thread obtained the lock before we were able to do so, retry
										}
										else {
											throw ae;
										}
									}
								}
							}
						}
					}
				}
				else {
					throw ae;
				}
			}
		}
		
	}
	
	/** 
	 * Release the lock on this record, but only if we own it. 
	 * <p>
	 * @param key - The key of the record on which the lock is to be released 
	 * @return -  true if this thread owned the lock and the lock was successfully released, false if this thread did not own the lock.
	 */
	public boolean releaseLock(WritePolicy writePolicy, Key key) {
		return this.updateRecordAndReleaseLock(writePolicy, key, 0, (Operation[]) null);
	}
	
	/** 
	 * Release the lock on this record, but only if we own it. 
	 * <p>
	 * @param key - The key of the record on which the lock is to be released 
	 * @return -  true if this thread owned the lock and the lock was successfully released, false if this thread did not own the lock.
	 */
	public boolean releaseLock(WritePolicy writePolicy, int expectedGenCount, Key key) {
		return this.updateRecordAndReleaseLock(writePolicy, key, expectedGenCount, (Operation[]) null);
	}
	
	/** 
	 * Release the lock on this record, but only if we own it. 
	 * <p>
	 * @param key - The key of the record on which the lock is to be released 
	 * @param operations - the other operations to be performed with releasing the lock. Can be null.
	 * @param expectedGenCount - if &gt; 0, then a gen check will be performed on the record to ensure it matches this.
	 * @return -  true if this thread owned the lock and the lock was successfully released, false if this thread did not own the lock.
	 */
	public boolean updateRecordAndReleaseLock(WritePolicy writePolicy, Key key, int expectedGenCount, Operation ... operations) {
		Operation[] ops = new Operation[1+(operations == null ? 0 : operations.length)];
		int index = 0;
		if (operations != null) {
			for (Operation thisOp : operations) {
				ops[index++] = thisOp;
			}
		}
		ops[index] = getReleaseLockOperation(null);
		if (expectedGenCount > 0) {
			if (writePolicy == null) {
				writePolicy = new WritePolicy();
			}
			writePolicy.generation = expectedGenCount;
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		}
		Record record = client.operate(writePolicy, key, ops);
		if (record != null) {
			return record.getList(this.lockBinName) != null && record.getList(this.lockBinName).size() == 1;
		}
		return false;
	}

}
