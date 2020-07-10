package com.aerospike.storage.subkeys;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class SortedSubkeyMap<K extends Comparable<K>> implements Comparator<K>, ISortedSubkeyMap<K> {
	/**
	 * The aim of this class is to provide 
	 * <p>
	 * definitions:
	 * <ul>
	 * <li>BLOCK - an element in the doubly linked list. It contains a map, keyed by the key of the sub map with a value of a list containing [ttl, digest] </li>
	 * <li>ROOT_MAP - A single item (for now) which stores a map containing [min -> block id]</li>
	 * </ul>
	 * 
	 * The BLOCKS are stored in a list under the primary key. So if the primary key is Users.Fred, the blocks will be stored in Users-subkeys with keys having 
	 * values like Users-subkeys.Fred-1, Users-subkeys.Fred-2, etc. The first block in the chain will ALWAYS have a block id of -1. 
	 * <p/>
	 * The maximum number of records in a block is controlled by the <code>maxElementsPerBlock</code> option. When the number of elements in the block
	 * exceeds this, the block needs to split into 2 smaller blocks. The size of a block should be picked so that splits are rare but the number
	 * of elements will not exceed the maximum record size.
	 * <p/>
	 * When a block splits, the elements in it are divided into 2 blocks, including the new element which caused the split. Imagine the block has a 
	 * number key, the block limit is 10 items and the current values are 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000. Assume a new value of 50 is 
	 * inserted. There are now 11 values in the map, which will cause it to split. The values are split into 2 blocks: 50, 100, 200, 300, 400, 500 and
	 * 600, 700, 800, 900, 1000. The first values are written into the first block, second values are written into the second block. The first block
	 * keeps the original id and has a pointer to the second block. The second block has a back pointer to the original block so that reverse scans
	 * can be used. 
	 * <p/>
	 * Since the maps themselves in the block are ordered, and the blocks form a (doubly) linked list with the previous blocks containing values strictly
	 * less than the values in this block, and the values in this block are strictly less than the values in the next block, we can find values in sorted
	 * order simply by traversing the values in the block and then traversing onto the next block and so on.
	 * <p/>
	 * In order to find the starting point for a particular key, we store a root block, which contains a map of (minimum value in block -> id of block).
	 * The block to use is the value in the map of the closest id less than the desired key. So imagine the map contains:
	 * <code>
	 * { a->1, d->2, f->5, r->4, z->3 }
	 * </code>
	 * If we want to find which block will contain "e", we would find block "d" as the smallest value in the map less than "e", and hence look in block 2.
	 */
	private static final Logger log = LogManager.getLogger(SortedSubkeyMap.class);
	private static final String SUBKEY_SETNAME_DIFFERENTIATOR = "-subkeys";
	static final String BLOCK_SETNAME_DIFFERENTIATOR = "-meta";
	private static final String SUBKEY_SEPARATOR = "-";
	private static final String EMPTY_BLOCK_PTR = "";
	
	private final SortedSubkeyMapOptions options;
	
	/* Definitions to be used for the locks */
	private static final String LOCK_BIN = "lck";
	
	private final AerospikeClient client;
	private final WritePolicy writePolicy;
	private final DistributedLockManager lockManager;
	
	private class SubRecordDigestContainer {
		public final List<Object> digests;
		public final String blockKey;
		public final K lastReadValue;
		public SubRecordDigestContainer(List<Object> digests, String blockKey, K lastReadValue) {
			super();
			this.digests = digests;
			this.blockKey = blockKey;
			this.lastReadValue = lastReadValue;
		}
	}
	
	// Package visible to allow unit tests
	static class MapSplitContainer<K> {
		final Map<Value, Value> firstHalfMap;
		final Map<Value, Value> secondHalfMap;
		final K splitMinValue;
		public MapSplitContainer(Map<Value, Value> firstHalfMap, Map<Value, Value> secondHalfMap, K splitMinValue) {
			super();
			this.firstHalfMap = firstHalfMap;
			this.secondHalfMap = secondHalfMap;
			this.splitMinValue = splitMinValue;
		}
	}
	
	public SortedSubkeyMap(AerospikeClient client) {
		this(client, false);
	}
	public SortedSubkeyMap(AerospikeClient client, boolean sendKey) {
		this(client, new SortedSubkeyMapOptions(sendKey));
	}
	
	public SortedSubkeyMap(AerospikeClient client, SortedSubkeyMapOptions options) {
		this.client = client;
		this.options = options;
		this.writePolicy = new WritePolicy();
		this.writePolicy.sendKey = this.options.isSendKey();
		this.lockManager = new DistributedLockManager(client, LOCK_BIN, options.getMaxLockTimeMs(), 1);
	}
	
	private Key getSubKey(Key parentKey, K subKey) {
		Value thisKey;
		if (parentKey.userKey.getType() == ParticleType.STRING) {
			thisKey = Value.get(parentKey.userKey.toString() + SUBKEY_SEPARATOR + subKey.toString());
		}
		else if (parentKey.userKey.getType() == ParticleType.INTEGER) {
			thisKey = Value.get(Long.toString(parentKey.userKey.toLong()) + SUBKEY_SEPARATOR + subKey.toString());
		}
		else {
			throw new IllegalArgumentException("Subkeys must either be Strings or Integers");
		}
		return new Key(parentKey.namespace, parentKey.setName+SUBKEY_SETNAME_DIFFERENTIATOR, thisKey); 
	}
	
	@Override
	public int compare(K o1, K o2) {
		return o1.compareTo(o2);
	}
	
	private long getIdOfFirstBlock(Key key) {
		Record result = client.operate(null, new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest),
				MapOperation.getByIndex(options.getRootMapBin(), 0, MapReturnType.VALUE));
		return result == null ? Long.MIN_VALUE : result.getLong(options.getRootMapBin());
	}
	
	private long getIdOfLastBlock(Key key) {
		Record result = client.operate(null, new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest),
				MapOperation.getByIndex(options.getRootMapBin(), -1, MapReturnType.VALUE));
		return result == null ? Long.MIN_VALUE : result.getLong(options.getRootMapBin());		
	}
	
	private Key getBlockKey(Key mainRecord, long blockId) {
		return new Key(mainRecord.namespace, mainRecord.setName + BLOCK_SETNAME_DIFFERENTIATOR, mainRecord.userKey + SUBKEY_SEPARATOR + blockId);
	}

	private Key getKeyFromBlockPointer(Key mainKey, String blockPointer) {
		return new Key(mainKey.namespace, mainKey.setName + BLOCK_SETNAME_DIFFERENTIATOR, blockPointer);
	}
	
	/**
	 * Given a Key, find out which Block may contain a subKey pertinent to that key. This is performed by consulting the 
	 * Root Map for that key, which contains a map of minValue -> block id.
	 * @param key
	 * @param subKey
	 * @return
	 */
	private long getIdOfBlockToUse(Key key, K subKey) {
		// First we get the right record in the ROOT_MAP. This is probably a memory based NS with persistence, and
		// we assume for now there is exactly 1 block here.
		// We want the block which contains this key. The format of this record is min_value_in_block -> [max_value_in_block, block_id]
		// Thus we can use getByKeyRelativeIndexRange to get the first block of the record.
		// Note that the way getByKeyRelativeIndexRange works is it will find the index which is greater than the passed value so
		// if the blocks contain [100,a], [200,b], [300,c] and we pass 150, we need the 100 block and will get the 200 block. So we need
		// to subtract 1 from the index to get to the 100 block. The one exception is if we pass actual minimum value of the block.
		// So passing 200 should give block 200, but the subtraction of 1 will give it block 100. 
		Record result = client.operate(null, new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest),
				MapOperation.getByKeyRelativeIndexRange(options.getRootMapBin(), Value.get(subKey), -1, 1, MapReturnType.KEY_VALUE),
				MapOperation.getByKey(options.getRootMapBin(), Value.get(subKey), MapReturnType.KEY_VALUE));
		
		if (result == null) {
			// The record did not exist, this is the first insert. Return null to let the caller handle this.
			return -1;
		}
		else {
			if (result.bins.containsKey(options.getRootMapBin())) {
				@SuppressWarnings("unchecked")
				List<Object> bothResults = (List<Object>) result.getList(options.getRootMapBin());
				List<SimpleEntry<K, Long>> exactValue = (List<SimpleEntry<K, Long>>) bothResults.get(1);
				List<SimpleEntry<K, Long>> item = (List<SimpleEntry<K, Long>>) bothResults.get(0);
				if (exactValue.size() > 0) {
					return exactValue.get(0).getValue();
				}
				if (item != null && item.size() > 0) {
					// There should be exactly 1 record in this map, with the key being the min value and the item being the block id
					return item.get(0).getValue();
				}
			}
			// This value falls before the first value in the set, return the first block (1)
			return 1;
		}
	}
		
	/**
	 * Initialize a new root block. This is used when subkeys have been added for the first time to
	 * a parent key. To do this, we need to create a new root block
	 * @param key
	 * @param subKey
	 * @param blockRecordData
	 */
	private void initalizeBlocks(Key key, K subKey, Value blockRecordData) {
		// Must insert a new block.
		long idOfBlockToUse = allocateId(key);
		
		WritePolicy wp = new WritePolicy();
		wp.sendKey = this.options.isSendKey();
		wp.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		
		MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0);
		Key blockKey = getBlockKey(key, idOfBlockToUse);
		client.operate(wp, blockKey, MapOperation.put(mp, options.getBlockMapBin(), Value.get(subKey), blockRecordData),
				Operation.put(new Bin(options.getBlockMapNextBin(), EMPTY_BLOCK_PTR)),
				Operation.put(new Bin(options.getBlockMapPrevBin(), EMPTY_BLOCK_PTR))
		);
		
		// Now create the first object in the ROOT map
		Key rootMapKey = new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest);
		wp.recordExistsAction = RecordExistsAction.UPDATE;

		client.operate(wp, rootMapKey, MapOperation.put(mp, options.getBlockMapBin(), Value.get(subKey), Value.get(idOfBlockToUse)));
	}

	/**
	 * Insert the record into the sorted ordering.
	 * <b>before</b> invoking this code.
	 * @param key
	 * @param subKey
	 * @param dataRecordWritePolcy - the write policy used to insert the data
	 * @param bins
	 */
	@Override
	public void put(Key key, K subKey, WritePolicy dataRecordWritePolicy, Bin ...bins) {
		put(key, subKey, dataRecordWritePolicy, null, bins);
	}
	
	/**
	 * Insert the record into the sorted ordering.
	 * <b>before</b> invoking this code.
	 * @param key
	 * @param subKey
	 * @param dataRecordWritePolcy - the write policy used to insert the data
	 * @param keyAssociatedWithData - Sometimes the digest of the record associate with the key is different to the parent key. This allows different data to be inserted. 
	 * 		Can be null, in which case the data inserted will be associated with the main key.
	 * @param bins
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void put(Key key, K subKey, WritePolicy dataRecordWritePolicy, Key keyAssociatedWithData, Bin ...bins) {
		if (dataRecordWritePolicy == null) {
			dataRecordWritePolicy = client.writePolicyDefault;
		}
		long idOfBlockToUse;
		long epoch = (dataRecordWritePolicy == null) ? Long.MAX_VALUE : (dataRecordWritePolicy.expiration <= 0) ? Long.MAX_VALUE : new Date().getTime() + dataRecordWritePolicy.expiration * 1000;

		Key blockKey;

		Key dataRecordKey = getSubKey(key, subKey);
		
		List<Object> blockRecordValues = new ArrayList<>();
		blockRecordValues.add(epoch);
		blockRecordValues.add(keyAssociatedWithData == null ? dataRecordKey.digest : keyAssociatedWithData.digest);
		Value blockRecordData = Value.get(blockRecordValues);
		while (true) {
			idOfBlockToUse = getIdOfBlockToUse(key, subKey);
			
			try {
				if (idOfBlockToUse == -1) {
					
					// Given the structure of the blocks, the only way we can get an id of -1
					// on a block is if there are no values in the map.
					this.initalizeBlocks(key, subKey, blockRecordData);
					
					// Now write the data
					if (bins != null && bins.length > 0) {
						client.put(dataRecordWritePolicy, dataRecordKey, bins);
					}
					return;
				}
				else {
					break;
				}
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
					// Another thread inserted the record whilst at the same time we did, retry everything
					continue;
				}
				else {
					throw ae;
				}
			}
		}

		// Now we have a valid block to use. Insert the record into the block map
		MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0);
		
		// The operations we must do are:
		// 1) Lock the record
		// 2) Get the count of the items in the map
		// 3) Insert the record (returns the map size)
		// 4) Get the index of the record we just inserted
		// 5) Unlock the record.
		// Then if we don't get a lock exception we know that:
		// a) If #2 = #3, this was an update operation so no need to adjust anything
		// b) Else if #4 == 0, this record contains the new minimum
		// c) Else if #2 == #4 (or #3-1), this record contains a new maximum
		// d) If (!a) and map size > allowable maximum, block must be split.
		Value subKeyValue = Value.get(subKey);
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.sendKey = this.options.isSendKey();
		writePolicy.expiration = dataRecordWritePolicy.expiration;
		
		// Set up retries, useful if an error occurs or the lock is already held
		writePolicy.sleepBetweenRetries = 5;
		writePolicy.maxRetries = 3;
		writePolicy.timeoutDelay = 20;
		
		blockKey = getBlockKey(key, idOfBlockToUse);

		Record record = lockManager.performOperationsUnderLock(writePolicy, blockKey,
				MapOperation.size(options.getBlockMapBin()),
				MapOperation.put(mp, options.getBlockMapBin(), subKeyValue, blockRecordData),
				MapOperation.getByKey(options.getBlockMapBin(), subKeyValue, MapReturnType.INDEX)
			);
		
		List<Long> data = (List<Long>) record.getList(options.getBlockMapBin());
		long originalCount = data.get(0);
		long updatedCount = data.get(1);
		Object secondObject = data.get(2);
		// CDTs currently have a bug where under particular conditions the index can be returned as an array with one element, not just a number
		// This has been fixed in development. (00015190)
		long insertedElementsIndex = (secondObject instanceof Number) ? data.get(2) : ((List<Long>)secondObject).get(0);
//		System.out.printf("Block: %d, element count: %d - ", idOfBlockToUse, updatedCount);
		if (originalCount == updatedCount) {
			// This was an update, no action necessary
//			System.out.println("Updated existing element " + subKey);
		}
		else {
			if (insertedElementsIndex == 0) {
				// In this case we have inserted a new minimum element, we need to update the Root Map
				Key rootMapKey = new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest);
				client.operate(null, rootMapKey,
						MapOperation.removeByValue(options.getBlockMapBin(), Value.get(idOfBlockToUse), MapReturnType.NONE),
						MapOperation.put(mp, options.getBlockMapBin(), Value.get(subKey), Value.get(idOfBlockToUse))	);

//				System.out.println("inserted new minimum element " + subKey);
			}
			else if (insertedElementsIndex == updatedCount-1) {
//				System.out.println("inserted new maximum element " + subKey);
			}
			else {
//				System.out.println("inserted new element " + subKey);
			}
			if (updatedCount > options.getMaxElementsPerBlock()) {
//				System.out.println("Block size == " + updatedCount + "! Need to split!");
				this.splitBlock(blockKey, key);
			}
		}
		// Now write the data
		if (bins != null && bins.length > 0) {
			client.put(dataRecordWritePolicy, dataRecordKey, bins);
		}
	}

	/**
	 * Return the next id in the sequence for this record. The id returned is the id of the next block, so this will
	 * be used when a block splits.
	 * @return
	 */
	private long allocateId(Key userKey) {
		// Use the digest here to encompass both the set name and id.
		final Key key = new Key(userKey.namespace, userKey.setName + BLOCK_SETNAME_DIFFERENTIATOR, userKey.digest);
		final String BIN_NAME = "id"; 
		Record record = client.operate(this.writePolicy, key, Operation.add(new Bin(BIN_NAME, Value.get(1))), Operation.get(BIN_NAME));
		return record.getLong(BIN_NAME);
	}
	
	/**
	 * This method will take map of values and split it so that half the values are in the original map
	 * and the other half of the values are in the returned map. The original map will contain the first half
	 * of the sorted keys and the returned map will contain the second half of the map sorted by keys
	 * @param existingValues
	 * @return
	 */
	private <V> MapSplitContainer<K> splitMap(TreeMap<K, V> existingValues) {
		if (existingValues.size() > 1) {
			Map<Value, Value> firstHalfMap = new HashMap<>(existingValues.size());
			Map<Value, Value> secondHalfMap = new HashMap<>(existingValues.size());
			int totalValues = existingValues.size();
			int splitPoint = (totalValues+1)/2;
			
			int i = 0;
			K splitValue = null;
			for (K key : existingValues.keySet()) {
				V thisValue = existingValues.get(key);
				if (i < splitPoint) {
					firstHalfMap.put(Value.get(key), Value.get(thisValue));
				}
				else {
					secondHalfMap.put(Value.get(key), Value.get(thisValue));
				}				
				if (i == splitPoint) {
					splitValue = key;
				}
				i++;
			}
			if (log.isDebugEnabled()) {
				log.debug(String.format("Splitting at index %d, first new %s", splitPoint, splitValue));
			}
			
			return new MapSplitContainer<K>(firstHalfMap, secondHalfMap, splitValue);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private void splitBlock(Key blockKey, Key topRecordKey) {
		// The data in the map associated with the passed key is too big. We need to split it into 2 blocks
		// First, get all the data in the block after acquiring the lock.
		Record record = lockManager.acquireLock(blockKey, options.getMaxLockTimeMs(), options.getBlockMapBin(), options.getBlockMapNextBin(), options.getBlockMapPrevBin(), LOCK_BIN);
		
		try {
			// Check to ensure we still need to split this block. It's possible that we queued behind another thread which was splitting the block and by the
			// time we acquired the lock the block was already split
			TreeMap<K, List<Object>> blockMap = (TreeMap<K, List<Object>>) record.getMap(options.getBlockMapBin());
			if (blockMap.size() <= options.getMaxElementsPerBlock()) {
				return;
			}
	
			MapSplitContainer<K> container = splitMap(blockMap); 
			K newMinValue = container.splitMinValue;
			
			// TODO: Could these operations all be done in parallel?
			WritePolicy updatePolicy = new WritePolicy();
			updatePolicy.sendKey = this.options.isSendKey();
//			replacePolicy.recordExistsAction = RecordExistsAction.REPLACE;
			
			// Step 1: Write the second block.
			// -----------------------------------
			// Set up the operations to insert the new block into the map.
			long id = allocateId(topRecordKey);
			String secondBlockId = topRecordKey.userKey + SUBKEY_SEPARATOR + id;
			Key secondBlockKey = new Key(blockKey.namespace, blockKey.setName, secondBlockId);
			// The previous pointer of the new block is this original block we just split from
			Bin prevPointerBin = new Bin(options.getBlockMapPrevBin(), blockKey.userKey);
			// The next pointer of the new block is the same as the next pointer of the original block (as the next pointer of this block will become this new block)
			Bin nextPointerBin = new Bin(options.getBlockMapNextBin(), record.getString(options.getBlockMapNextBin()));
			
			// Now write the new block. It's not properly linked into the chain yet.
			client.operate(updatePolicy, secondBlockKey, 
					MapOperation.putItems(new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0), options.getBlockMapBin(), container.secondHalfMap),
					Operation.put(prevPointerBin),
					Operation.put(nextPointerBin));
//			client.put(replacePolicy, secondBlockKey, new Bin(options.getBlockMapBin(), Value.get(secondBlockMap)), prevPointerBin, nextPointerBin);
			

			// Step 2: Update the root block to include this second block
			// ------------------------------------------------------------
			client.operate(writePolicy, new Key(options.getRootMapNamespace(topRecordKey), options.getRootMapSet(topRecordKey), topRecordKey.digest), 
					MapOperation.put(new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0), options.getRootMapBin(), Value.get(newMinValue), Value.get(id)));

			// Step 3: Update the original block with the new map and new next pointer
			// -----------------------------------------------------------------------
			// Note that the prev pointer won't have changed, but it's more efficient to do a REPLACE and pass the prior value.
			// NOTE: Since this code will replace the LOCK bin too, the lock will be effectively removed. This means that step 4
			// will be done outside of the context of the lock, but since it's only splitting code which updates the back pointer
			// (ie the new block effectively owns the back pointer of the next block) this should not be an issue.
			//
			// Note: Could not implement this as the CDT operation threw an exception. Log message:
			// WARNING (rw): (write.c:977) {test} write_master: cdt modify op can't have record-level replace flag <Digest>:0xc10c5d62b7b7b527d88c4940d3d94e8e8c40a99b
			// Hence used update
			client.operate(updatePolicy, blockKey,
					MapOperation.clear(options.getBlockMapBin()),
					MapOperation.putItems(new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0), options.getBlockMapBin(), container.firstHalfMap),
					Operation.put(new Bin(options.getBlockMapNextBin(), secondBlockId)),
					Operation.put(new Bin(options.getBlockMapPrevBin(), record.getString(options.getBlockMapPrevBin()))));
					
//			client.put(replacePolicy, blockKey, new Bin(options.getBlockMapBin(), Value.get(blockMap)), 
//					new Bin(options.getBlockMapNextBin(), secondBlockId),
//					new Bin(options.getBlockMapPrevBin(), record.getString(options.getBlockMapPrevBin())));
			
			// Step 4: Update the back pointer of the next block (if there is one) to point to our second block
			// ------------------------------------------------------------------------------------------------
			String nextBlock = record.getString(options.getBlockMapNextBin());
			if (!EMPTY_BLOCK_PTR.equals(nextBlock)) {
				client.put(writePolicy, new Key(blockKey.namespace, blockKey.setName, nextBlock), new Bin(options.getBlockMapPrevBin(), secondBlockId));
			}
		}
		finally {
			lockManager.releaseLock(null, blockKey);
		}
	}
	
	private Operation readWholeBlockOperation(Key mainRecord, boolean forwards, int count) {
		Operation operation;
		if (forwards) {
			if (count > 0) {
				operation = MapOperation.getByIndexRange(options.getBlockMapBin(), 0, count, MapReturnType.KEY_VALUE);
			}
			else {
				operation = MapOperation.getByIndexRange(options.getBlockMapBin(), 0,  MapReturnType.KEY_VALUE);
			}
		}
		else {
			if (count > 0) {
				operation = MapOperation.getByIndexRange(options.getBlockMapBin(), -count, count, MapReturnType.KEY_VALUE);
			}
			else {
				operation = MapOperation.getByIndexRange(options.getBlockMapBin(), -1000000, 1000000, MapReturnType.KEY_VALUE);
			}			
		}
		return operation;
	}
	
	private Operation getReadOperation(Key mainRecord, K firstKey, boolean includeFirst, boolean forwards, int max) {
		Operation op;
		if (firstKey == null) {
			op = readWholeBlockOperation(mainRecord, forwards, max);
		}
		else {
			if (forwards) {
				if (max > 0) {
					op = MapOperation.getByKeyRelativeIndexRange(options.getBlockMapBin(), Value.get(firstKey), 0, max+1, MapReturnType.KEY_VALUE);
				}
				else {
					op = MapOperation.getByKeyRelativeIndexRange(options.getBlockMapBin(), Value.get(firstKey), includeFirst ? 0 : 1, MapReturnType.KEY_VALUE);						
				}
			}
			else {
				if (max > 0) {
					op = MapOperation.getByKeyRelativeIndexRange(options.getBlockMapBin(), Value.get(firstKey), -max , max+1, MapReturnType.KEY_VALUE);
				}
				else {
					op = MapOperation.getByKeyRelativeIndexRange(options.getBlockMapBin(), Value.get(firstKey), includeFirst ? 1 : 0, MapReturnType.KEY_VALUE | MapReturnType.INVERTED);
				}
			}
		}
		return op;
	}
	
	private K addValidDigestsToList(List<SimpleEntry<K, List<Object>>> aList, List<Object> digests, boolean forwards, long epoch, int max, boolean includeFirst, K lastKey) {
		K newLastKey = null;
		int size = aList.size();
		int i = forwards ? 0 : size - 1;
		while (forwards ? (i < size) : (i >=0)) {
			K currentKey = aList.get(i).getKey();
			List<Object> values = (List<Object>) aList.get(i).getValue();
			boolean addToList = true;
			if (lastKey != null) {
				int compareResult = currentKey.compareTo(lastKey);
				if ((compareResult == 0 && !includeFirst) || (forwards && compareResult < 0) || (!forwards && (compareResult > 0))) {
					addToList = false;
				}
			}
			if ( addToList && (long)values.get(0) > epoch) {
				// This record has not TTLd out. Probably.
				digests.add(values.get(1));
				newLastKey = currentKey;
				if (digests.size() >= max) {
					break;
				}
			}
			i += (forwards ? 1 : -1);
		}
		return newLastKey;
	}

	@SuppressWarnings("unchecked")
	private SubRecordDigestContainer getSubRecordDigests(Key mainRecord, K firstKey, boolean includeFirst, boolean forwards, int maxToFetch, long epoch) {
		boolean debug = log.isDebugEnabled();
		long blockId;
		List<Object> digests = new ArrayList<>(maxToFetch);
		K lastKey = null;
		
		
		if (firstKey == null) {
			blockId = forwards ? getIdOfFirstBlock(mainRecord) : getIdOfLastBlock(mainRecord);
			if (blockId == Long.MIN_VALUE) {
				// There are no records
				return new SubRecordDigestContainer(new ArrayList<>(), null, null);
			}
		}
		else {
			blockId = getIdOfBlockToUse(mainRecord, firstKey);
		}
		Operation op = getReadOperation(mainRecord, firstKey, includeFirst, forwards, maxToFetch);
		Operation getNextOp = Operation.get(options.getBlockMapNextBin());
		Operation getPrevOp = Operation.get(options.getBlockMapPrevBin());
		if (debug) {
			log.debug(String.format("[%d]: getSubRecordDigests(%s, %s, includeFirst=%b, forwards=%b, maxToFetch=%d, epoch=%d) gives block %d\n",
					Thread.currentThread().getId(), mainRecord, firstKey, includeFirst, forwards, maxToFetch, epoch, blockId));
		}
		
		Key key = getBlockKey(mainRecord, blockId);
		Record rec = client.operate(null, key, op, getNextOp, getPrevOp);
		
		List<SimpleEntry<K, List<Object>>> aList = (List<SimpleEntry<K, List<Object>>>) rec.getList(options.getBlockMapBin());
		lastKey = addValidDigestsToList(aList, digests, forwards, epoch, maxToFetch, includeFirst, firstKey);
		int remaining = maxToFetch - digests.size();

		if (debug) {
			log.debug(String.format("   - %d items read this iteration, total valid digests = %d, remaining = %d, lastKey = %s", aList.size(), digests.size(), remaining, ""+lastKey));
		}

		String nextKey = key.userKey.toString();
		
		while (remaining > 0) {
			// We need to load more records
			if (aList.size() < maxToFetch) { 
				// we have all the records in this block, move onto the next block.
				if (forwards) {
					nextKey = rec.getString(options.getBlockMapNextBin());
				}
				else {
					nextKey = rec.getString(options.getBlockMapPrevBin());
				}
				if (nextKey == null || nextKey.isEmpty()) {
					if (debug) {
						log.debug("   - No block, bailing out.");
					}
					// Fetched all the records, abort.
					break;
				}
				if (debug) {
					log.debug(String.format("   - Moving to next block, reading block %s", nextKey));
				}
				op = getReadOperation(mainRecord, null, false, forwards, remaining);
				rec = client.operate(null, getKeyFromBlockPointer(mainRecord, nextKey), op, getNextOp, getPrevOp);
				
				aList = (List<SimpleEntry<K, List<Object>>>) rec.getList(options.getBlockMapBin());
				lastKey = addValidDigestsToList(aList, digests, forwards, epoch, maxToFetch, false, lastKey);
			}
			else {
				// There are possibly more in this block.
				op = getReadOperation(mainRecord, lastKey, false, forwards, remaining);
				rec = client.operate(null, key, op, getNextOp, getPrevOp);
				aList = (List<SimpleEntry<K, List<Object>>>) rec.getList(options.getBlockMapBin());
				lastKey = addValidDigestsToList(aList, digests, forwards, epoch, maxToFetch, false, lastKey);
			}
			remaining = maxToFetch - digests.size();
			if (debug) {
				log.debug(String.format("   - %d items read this iteration, total valid digests = %d, remaining = %d, lastKey = %s", aList.size(), digests.size(), remaining, ""+lastKey));
			}
		}
		return new SubRecordDigestContainer(digests, nextKey, lastKey);
	}
	
	@Override
	public ResultsContainer<K> getSubRecords(ContinuationContainter<K> continuation, int max) {
		return getSubRecords(continuation.getMainRecordKey(), continuation.getLastReadKey(), false, continuation.isForwards(), max);
	}
	
	@Override
	public ResultsContainer<K> getSubRecords(Key mainRecord, K firstKey, boolean includeFirst, boolean forwards, int max) {
		if (max <= 0) {
			throw new IllegalArgumentException("The maximum number of records must be specified");
		}
		
		// Due to the possibility of records expiring or being deleted behind the scenes, we will try to get more records 
		// than asked for and trim the excess.
		// TODO: Fix this up, we might need to trim the records down to exactly what is asked for.
		//int maxToFetch = (int)(max * 1.2);
		int maxToFetch = (int)(max * 1);
		
		long epoch = new Date().getTime();
		
		SubRecordDigestContainer digestContainer = getSubRecordDigests(mainRecord, firstKey, includeFirst, forwards, maxToFetch, epoch);
		
		List<Object> digests = digestContainer.digests;
		Key[] keys = new Key[digests.size()];
		String setName = mainRecord.setName + SUBKEY_SETNAME_DIFFERENTIATOR;
		for (int i = 0; i <  keys.length; i++) {
			keys[i] = new Key(mainRecord.namespace, (byte [])digests.get(i), setName, null);
		}
		Record[] results =  client.get(null, keys);
		
		// TODO: some of the records might be NULL because they've TTLd out or been deleted. Do we care at this point?
		if (log.isDebugEnabled()) {
			for (int i = 0; i < results.length; i++) {
				log.debug(String.format("%d - %s", i, results[i] == null ? "null" : results[i]));
			}
		}
		return new ResultsContainer<K>(new ContinuationContainter<K>(mainRecord, digestContainer.blockKey, digestContainer.lastReadValue, forwards), results);
	}
	
	private void removeEmptyBlock(Key key, long idOfBlockToUse, WritePolicy deleteWritePolicy) {
		Key blockKey = getBlockKey(key, idOfBlockToUse);
		if (idOfBlockToUse == 1) {
			// Block 1 is a special case here as we always want a constant start block
			// TODO: Code this
		}
		else {
			// If the map is empty, we should remove this block by adjusting indexes
			// Step 1: Lock the block and confirm that it is still empty
			Record record = lockManager.acquireLock(blockKey, options.getMaxLockTimeMs(), options.getBlockMapBin(), options.getBlockMapNextBin(), options.getBlockMapPrevBin());
			int size = record.getMap(options.getBlockMapBin()).size();
			String nextBlock = record.getString(options.getBlockMapNextBin());
			String prevBlock = record.getString(options.getBlockMapPrevBin());

			Key prevBlockKey = getKeyFromBlockPointer(key, prevBlock);
			Key nextBlockKey = getKeyFromBlockPointer(key, nextBlock);
			
			if (size == 0) {
				// We need this check as there is a possibility that we deleted an item but a new one was inserted after this before we acquired the lock.
				if (EMPTY_BLOCK_PTR.equals(nextBlock)) {
					// This block was at the end of the chain, we can just update the next pointer on the previous block and remove this value from the root map
					lockManager.performOperationsUnderLock(deleteWritePolicy, prevBlockKey, Operation.put(new Bin(options.getBlockMapNextBin(), EMPTY_BLOCK_PTR)));
				}
				else {
					// This block was in the middle of the list. Adjust the previous block's next point and the next block's previous pointer
					lockManager.performOperationsUnderLock(deleteWritePolicy, prevBlockKey, Operation.put(new Bin(options.getBlockMapNextBin(), nextBlock)));
					lockManager.performOperationsUnderLock(deleteWritePolicy, nextBlockKey, Operation.put(new Bin(options.getBlockMapPrevBin(), prevBlock)));					
				}
				Key rootMapKey = new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest);
				client.operate(null, rootMapKey,
						MapOperation.removeByValue(options.getBlockMapBin(), Value.get(idOfBlockToUse), MapReturnType.NONE));
				
				// The block should be de-linked, we can just remove it.
				client.delete(deleteWritePolicy, key);
			}
			lockManager.releaseLock(null, blockKey);
		}
	}
	
	private static final String MAX_STRING_VALUE = "\uffff";
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean delete(Key key, K subKey, WritePolicy deleteWritePolicy) {
		if (deleteWritePolicy == null) {
			deleteWritePolicy = client.writePolicyDefault;
		}
		long idOfBlockToUse = getIdOfBlockToUse(key, subKey);
		MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0);

		Value subKeyValue = Value.get(subKey);
		Value maxStringValue = Value.get(MAX_STRING_VALUE);
		String binName = options.getBlockMapBin();
		
		// We have to move this key out of the appropriate block
		// This is not as simple as it seems. We need to remove the value in the block, be able to identify if it
		// was the first value in the map and if it was, what the new first value in the map is. Atomically. To 
		// do this we need to insert a new value in the map which will be the last value in the map, remove the item
		// we want to remove, get the value of the first value in the map and then remove the element we put in.
		Record result = lockManager.performOperationsUnderLock(deleteWritePolicy, getBlockKey(key, idOfBlockToUse),
				MapOperation.put(mp, binName, maxStringValue, maxStringValue),
				MapOperation.removeByKey(binName, subKeyValue, MapReturnType.INDEX),
				MapOperation.getByIndex(binName, 0, MapReturnType.KEY),
				MapOperation.removeByKey(binName, maxStringValue, MapReturnType.NONE)
			);

		List<Object> data = (List<Object>) result.getList("map");
		long removedObjectIndex = (long) data.get(1);
		Object newMinimum = data.get(2);
		
		if (removedObjectIndex == -1) {
			// Value not found, just do nothing
			System.out.println("Value was not found");
			return false;
		}
		else if (MAX_STRING_VALUE.equals(newMinimum)) {
			System.out.println("Value was removed, map is now empty");
			removeEmptyBlock(key, idOfBlockToUse, deleteWritePolicy);
		}
		else if (removedObjectIndex == 0) {
			System.out.println("Removed minimum value, new minimum = " + newMinimum);
			// In this case, we need to update the minimum value for this block in the root map
			Key rootMapKey = new Key(options.getRootMapNamespace(key), options.getRootMapSet(key), key.digest);
			client.operate(null, rootMapKey,
					MapOperation.removeByValue(options.getBlockMapBin(), Value.get(idOfBlockToUse), MapReturnType.NONE),
					MapOperation.put(mp, options.getBlockMapBin(), Value.get(newMinimum), Value.get(idOfBlockToUse)));
		}

		Key compoundKey = getSubKey(key, subKey);
		return client.delete(writePolicy, compoundKey);
	}
	
	@Override
	public Record get(Key key, K subKey, Policy readPolicy) {
		Key compoundKey = getSubKey(key, subKey);
		return client.get(writePolicy, compoundKey);
	}

	
	@Override
	public void rebuildRootBlockAndValidatePointers(Key key) {
		// The first block in the list is ALWAYS block 1
		long blockId = 1;
		String prevBlock = EMPTY_BLOCK_PTR;
		Key blockKey = getBlockKey(key, blockId);
		Record record;
		do {
			record = client.operate(null, blockKey, 
					MapOperation.getByIndex(options.getBlockMapBin(), 0, MapReturnType.KEY),
					Operation.get(options.getBlockMapNextBin()),
					Operation.get(options.getBlockMapPrevBin())
				);
			
			Object minValue = record.getValue(options.getBlockMapBin());
			System.out.printf("Key: %s, previous: %s, minValue: %s\n", blockKey.userKey.toString(), prevBlock, minValue.toString());
			prevBlock = blockKey.userKey.toString();
			blockKey = getKeyFromBlockPointer(key, record.getString(options.getBlockMapBin())); 
		}
		while (record != null);
	}
}
