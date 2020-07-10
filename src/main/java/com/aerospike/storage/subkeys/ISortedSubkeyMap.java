package com.aerospike.storage.subkeys;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public interface ISortedSubkeyMap<K> {

	boolean delete(Key key, K subKey, WritePolicy deleteWritePolicy);
	Record get(Key key, K subKey, Policy readPolicy);
	
	void put(Key key, K subKey, WritePolicy dataRecordWritePolicy, Bin ... bins);
	void put(Key key, K subKey, WritePolicy dataRecordWritePolicy, Key keyAssociatedWithData, Bin ... bins);

	ResultsContainer<K> getSubRecords(Key mainRecord, K firstKey, boolean includeFirst, boolean forwards, int max);
	ResultsContainer<K> getSubRecords(ContinuationContainter<K> continuation, int max);
	
	/**
	 * This administration function rebuilds the root block for a given key, throwing away the existing
	 * data. It also resets any incorrectly set back pointers.
	 * <p/>
	 * This function does not take locks but assumes that no writes are occurring when it's running.
	 * @param key
	 */
	void rebuildRootBlockAndValidatePointers(Key key);
}
