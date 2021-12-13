package com.aerospike.storage.subkeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class TestAerospikeCdts {

	private final String HOST = "127.0.0.1";
	private AerospikeClient client;

	@Before
	public void setUp() throws Exception {
		 client = new AerospikeClient(HOST, 3000);
	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}

	@Test
	public void testKey() {
		Key key = new Key("test", "testKey", "test");
		
		MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0);

		// Clean up before the test.
		client.delete(null, key);

		// No records in the map, this returns null
		Record record = client.operate(null, key, MapOperation.getByKey("name", Value.get("MapKey"), MapReturnType.INDEX));
		System.out.println(record);
		assertNull(record);
		
		// Insert 1st value into the list
		client.operate(null, key, MapOperation.put(mp, "name", Value.get("MapKey"), Value.get("MapValue")));
		// Just one entry in the map which we're looking up -- This returns "name:[0]" -- Why the list?
		record = client.operate(null, key, MapOperation.getByKey("name", Value.get("MapKey"), MapReturnType.INDEX));
		System.out.println(record);
//		long index = record.getLong("name");
//		assertEquals(0, index);
		
		client.operate(null, key, MapOperation.put(mp, "name", Value.get("MapKey2"), Value.get("MapValue2")));
		// Two entries in the list -- but the same lookup as above returns "name:0" -- which is expected
		record = client.operate(null, key, MapOperation.getByKey("name", Value.get("MapKey"), MapReturnType.INDEX));
		System.out.println(record);
		long index1 = record.getLong("name");
		assertEquals(0, index1);
	}
	
	@Test
	public void testReplaceMap() {
		Key key = new Key("test", "testKey", "test");
		WritePolicy replaceWritePolicy = new WritePolicy();
		replaceWritePolicy.recordExistsAction = RecordExistsAction.REPLACE;
		
		WritePolicy updateWritePolicy = new WritePolicy();
		updateWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		
		MapPolicy mp = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, 0);
		client.delete(null, key);
		
		client.operate(updateWritePolicy, key, MapOperation.put(mp, "name", Value.get("MapKey"), Value.get("MapValue")));
		// This throws a parameter error. In the logs is the error:
		// WARNING (rw): (write.c:1066) {test} write_master: cdt modify op can't have record-level replace flag <Digest>:0x2077d96d27131e66b1ab4f7991fdfcb49251081e
		client.operate(replaceWritePolicy, key, MapOperation.put(mp, "name", Value.get("MapKey"), Value.get("MapValue")));
		
	}

}
