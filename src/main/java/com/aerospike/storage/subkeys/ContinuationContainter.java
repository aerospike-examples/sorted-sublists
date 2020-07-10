package com.aerospike.storage.subkeys;

import com.aerospike.client.Key;

/**
 * This class allows searches to be repeated from the last stopping point. So if for example 200 records
 * are asked for in a forwards direction, an instance of this class is returned from the search and allows
 * the next 200 records to be retrieved.
 * @author Tim
 *
 * @param <K>
 */
public class ContinuationContainter<K> {
	final private String currentBlock;
	final private K lastReadKey;
	final private boolean forwards;
	final private Key mainRecordKey;
	
	public ContinuationContainter(Key mainRecordKey, String currentBlock, K lastReadKey, boolean forwards) {
		super();
		this.currentBlock = currentBlock;
		this.lastReadKey = lastReadKey;
		this.forwards = forwards;
		this.mainRecordKey = mainRecordKey;
	}

	public String getCurrentBlock() {
		return currentBlock;
	}

	public K getLastReadKey() {
		return lastReadKey;
	}

	public boolean isForwards() {
		return forwards;
	}
	
	public Key getMainRecordKey() {
		return mainRecordKey;
	}
	
	public boolean isAtEnd() {
		return this.currentBlock == null || this.currentBlock.isEmpty();
	}
}
