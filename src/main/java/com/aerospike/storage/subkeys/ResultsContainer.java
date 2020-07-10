package com.aerospike.storage.subkeys;

import com.aerospike.client.Record;

public class ResultsContainer<K> {
	final private ContinuationContainter<K> continuationContainer;
	final private Record[] records;
	
	public ResultsContainer(ContinuationContainter<K> continuationContainer, Record[] records) {
		super();
		this.continuationContainer = continuationContainer;
		this.records = records;
	}

	public ContinuationContainter<K> getContinuationContainer() {
		return continuationContainer;
	}

	public Record[] getRecords() {
		return records;
	}
}
