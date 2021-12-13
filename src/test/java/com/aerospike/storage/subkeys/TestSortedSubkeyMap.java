package com.aerospike.storage.subkeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.storage.subkeys.SortedSubkeyMap.MapSplitContainer;


public class TestSortedSubkeyMap {

	private SortedSubkeyMap<String> stringMap;
	private SortedSubkeyMap<Long> longMap;
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
	@SuppressWarnings("unchecked")
	public void testMapSplit() throws Exception {
		// Test the sorted map
		stringMap = new SortedSubkeyMap<>(client);
		
		TreeMap<String, String> testMap = new TreeMap<>();
		testMap.put("12345", "test12345");
		testMap.put("98763", "test98763");
		testMap.put("22222", "test22222");
		testMap.put("11111", "test11111");
		testMap.put("45454", "test45454");
		testMap.put("66777", "test66777");
		testMap.put("88888", "test88888");
		
		System.out.println("Map before: " + testMap);
		Method m = stringMap.getClass().getDeclaredMethod("splitMap", TreeMap.class);
		m.setAccessible(true);
		SortedSubkeyMap.MapSplitContainer<String> container = (MapSplitContainer<String>) m.invoke(stringMap, testMap);
		System.out.println("Map after (1st): " + container.firstHalfMap);
		System.out.println("Map after (2nd): " + container.secondHalfMap);
		System.out.println("Split value: " + container.splitMinValue);
		assertEquals(4, container.firstHalfMap.size());
		assertEquals(3, container.secondHalfMap.size());
		assertEquals("66777", container.splitMinValue);
		for (Value v : container.firstHalfMap.keySet()) {
			assertTrue("Value " + v + " should be less than " + container.splitMinValue, "66777".compareTo(v.toString()) > 0);
		}
	}
	
	private SortedSubkeyMap<String> setupTestMap(Key mainKey) {
		SortedSubkeyMap<String> map = new SortedSubkeyMap<String>(client, true);

		// Clean up after prior tests
		client.truncate(null, "test", "rootMap", null);
		client.truncate(null, "test", "Users", null);
		client.truncate(null, "test", "Users-subkeys", null);
		client.truncate(null, "test", "Users-meta", null);

		//map.createHeaderRecord((long)1234);
		WritePolicy wp = new WritePolicy();
		wp.sendKey = true;
		wp.expiration = 86400;
		
		client.put(wp, mainKey, new Bin("name", Value.get("Tim")));
		
		map.put(mainKey, "1", wp, new Bin("message", "message-1"));
		map.put(mainKey, "5", wp, new Bin("message", "message-5"));
		map.put(mainKey, "3", wp, new Bin("message", "message-3"));
		map.put(mainKey, "7", wp, new Bin("message", "message-7"));
		map.put(mainKey, "5", wp, new Bin("message", "message-5.1"));
		map.put(mainKey, "0", wp, new Bin("message", "message-0"));

		for (int i = 10; i < 20; i++) {
			map.put(mainKey, ""+i, wp, new Bin("message", "message-"+i));
		}
		map.put(mainKey, "aaa", wp, new Bin("message", "message-aaa"));
		map.put(mainKey, "zzz", wp, new Bin("message", "message-zzz"));
		map.put(mainKey, "zebra", wp, new Bin("message", "message-zebra"));
		map.put(mainKey, "yankee", wp, new Bin("message", "message-yankee"));
		map.put(mainKey, "yak", wp, new Bin("message", "message-yak"));
		map.put(mainKey, "yuk", wp, new Bin("message", "message-yuk"));
		map.put(mainKey, "yonder", wp, new Bin("message", "message-yonder"));
		map.put(mainKey, "yellow", wp, new Bin("message", "message-yellow"));
		
		map.put(mainKey, "green", wp, new Bin("message", "message-green"));
		map.put(mainKey, "blue", wp, new Bin("message", "message-blue"));
		map.put(mainKey, "red", wp, new Bin("message", "message-red"));
		map.put(mainKey, "scarlet", wp, new Bin("message", "message-scarlet"));
		map.put(mainKey, "pink", wp, new Bin("message", "message-pink"));
		map.put(mainKey, "indigo", wp, new Bin("message", "message-indigo"));
		map.put(mainKey, "violet", wp, new Bin("message", "message-violet"));
		map.put(mainKey, "puce", wp, new Bin("message", "message-puce"));
		map.put(mainKey, "black", wp, new Bin("message", "message-black"));
		map.put(mainKey, "white", wp, new Bin("message", "message-white"));
		map.put(mainKey, "tule", wp, new Bin("message", "message-tule"));
		map.put(mainKey, "orange", wp, new Bin("message", "message-orange"));
		map.put(mainKey, "mandarin", wp, new Bin("message", "message-mandarin"));
		return map;
	}
	
	@Test
	public void testInsertsAndSplits() {
		Key mainKey = new Key("test", "Users", "Tim");

		SortedSubkeyMap<String> map = setupTestMap(mainKey);
		
		// Get all the records, check they're sorted ascending
		ResultsContainer<String> results = map.getSubRecords(mainKey, null, true, true, 100);
		Record[] records = results.getRecords();
		assertEquals(36, records.length);
		for (int i = 0; i < records.length-1; i++) {
			assertTrue("Records are not sorted!", records[i].getString("message").compareTo(records[i+1].getString("message")) < 0);
		}

		// Get all the records, check they're sorted ascending
		results = map.getSubRecords(mainKey, null, true, false, 100);
		records = results.getRecords();
		assertEquals(36, records.length);
		for (int i = 0; i < records.length-1; i++) {
			assertTrue("Records are not sorted!", records[i].getString("message").compareTo(records[i+1].getString("message")) > 0);
		}
		
		records = map.getSubRecords(mainKey, "tule", true, true, 10).getRecords();
		assertEquals(10, records.length);
		for (int i = 0; i < records.length-1; i++) {
			assertTrue("Records are not sorted!", records[i].getString("message").compareTo(records[i+1].getString("message")) < 0);
		}
		assertEquals("message-tule", records[0].getString("message"));
		
		// We ask for 10 records, but in this case we should only get 9 as there are no extra records
		records = map.getSubRecords(mainKey, "tule", false, true, 10).getRecords();
		assertEquals(9, records.length);
		for (int i = 0; i < records.length-1; i++) {
			assertTrue("Records are not sorted!", records[i].getString("message").compareTo(records[i+1].getString("message")) < 0);
		}
		assertEquals("message-violet", records[0].getString("message"));
		

		// Test 
		results = map.getSubRecords(mainKey, "tule", true, false, 4);
		int fetchCount = 1;
		do {
			if (fetchCount > 1) {
				results = map.getSubRecords(results.getContinuationContainer(), 4);
			}
			records = results.getRecords();
			System.out.println("Read #" + (fetchCount++) + ", records read = " + records.length);
			for (Record rr : records) {
				if (rr == null) {
					System.out.println("  - null");
				}
				else {
					System.out.println("  - " + rr.getString("message"));
				}
			}
		} while (!results.getContinuationContainer().isAtEnd());
	}
	
	@Test
	public void testLongMap() {
		client.truncate(null, "test", "Messages", null);
		client.truncate(null, "test", "Messages-meta", null);
		client.truncate(null, "test", "Messages-subkeys", null);
		client.truncate(null, "test", "rootMap", null);

		// This should initialize the sorted map to have 15 items per block, sending key to the server,
		// and using the same namespace and set for the root map as the main key.
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(15, null, null, true);
		this.longMap = new SortedSubkeyMap<>(client, options);
		
		Key key = new Key("test", "Messages", "Tim");
		client.put(null, key, new Bin("Name", "Tim"), new Bin("Age", 111));

		for (long i = 0; i < 100; i++) {
			this.longMap.put(key, i*1000, null, new Bin("Id", i*1000), new Bin("Message", "Message-" + i));
		}
		for (long i = 0; i < 100; i++) {
			this.longMap.put(key, i*1000 + 500, null, new Bin("Id", i*1000+500), new Bin("Message", "Message-" + i));
		}
		
		// Page through the results in pages of 7, going forwards
		assertEquals(100000, iterate(0, key, null, true, true, 7));

		// Page through the results in pages of 7, going backwards
		assertEquals(-500, iterate(99500, key, null, true, false, 7));
		
		// Start midrange not on an exact value. Page through the results in pages of 7, going forwards
		assertEquals(100000, iterate(50500, key, 50010L, true, true, 7));

		// Start midrange on an exact value. Page through the results in pages of 7, going forwards
		assertEquals(100000, iterate(50000, key, 50000L, true, true, 7));

		// Start midrange not on an exact value. Page through the results in pages of 7, going backwards
		assertEquals(-500, iterate(50000, key, 50010L, true, false, 7));

		// Start midrange on an exact value. Page through the results in pages of 7, going backwards
		assertEquals(-500, iterate(50000L, key, 50000L, true, false, 7));
		
		// Start midrange not on an exact value, do not include initial value. Page through the results in pages of 7, going forwards
		assertEquals(100000, iterate(50500, key, 50010L, false, true, 7));

		// Start midrange on an exact value, do not include initial value. Page through the results in pages of 7, going forwards
		assertEquals(100000, iterate(50500, key, 50000L, false, true, 7));

		// Start midrange not on an exact value, do not include initial value. Page through the results in pages of 7, going backwards
		assertEquals(-500, iterate(50000, key, 50010L, false, false, 7));

		// Start midrange on an exact value, do not include initial value. Page through the results in pages of 7, going backwards
		assertEquals(-500, iterate(49500, key, 50000L, false, false, 7));
		

		// get all results, going forwards
		assertEquals(100000, iterate(0, key, null, true, true, 0));

		// Get all results, going backwards
		assertEquals(-500, iterate(99500, key, null, true, false, 0));
		
		// Start midrange not on an exact value. Get all results, going forwards
		assertEquals(100000, iterate(50500, key, 50010L, true, true, 0));

		// Start midrange on an exact value. Get all results, going forwards
		assertEquals(100000, iterate(50000, key, 50000L, true, true, 0));

		// Start midrange not on an exact value. Get all results, going backwards
		assertEquals(-500, iterate(50000, key, 50010L, true, false, 0));

		// Start midrange on an exact value. Get all results, going backwards
		assertEquals(-500, iterate(50000L, key, 50000L, true, false, 0));
		
		// Start midrange not on an exact value, do not include initial value. Get all results, going forwards
		assertEquals(100000, iterate(50500, key, 50010L, false, true, 0));

		// Start midrange on an exact value, do not include initial value. Get all results, going forwards
		assertEquals(100000, iterate(50500, key, 50000L, false, true, 0));

		// Start midrange not on an exact value, do not include initial value. Get all results, going backwards
		assertEquals(-500, iterate(50000, key, 50010L, false, false, 0));

		// Start midrange on an exact value, do not include initial value. Get all results, going backwards
		assertEquals(-500, iterate(49500, key, 50000L, false, false, 0));
		
	}
	
	private long iterate(long expectedInitalValue, Key key, Long startValue, boolean includeFirst, boolean forwards, long incr) {
		long currentExpectedValue = expectedInitalValue;
		ResultsContainer<Long> results = this.longMap.getSubRecords(key, startValue, includeFirst, forwards, 7);
		while (true) {
			Record[] records = results.getRecords();
			for (Record r : records) {
				long id = r.getLong("Id");
				assertEquals(currentExpectedValue, id);
				if (forwards) {
					currentExpectedValue += 500;
				}
				else {
					currentExpectedValue -= 500;
				}
			}
			ContinuationContainter<Long> cont = results.getContinuationContainer();
			if (cont.isAtEnd()) {
				break;
			}
			results = this.longMap.getSubRecords(cont, 7);
		}
		return currentExpectedValue;
	}
	
	@Test
	public void testInsertAndDelete() {
		client.truncate(null, "test", "Messages", null);
		client.truncate(null, "test", "Messages-meta", null);
		client.truncate(null, "test", "Messages-subkeys", null);
		client.truncate(null, "test", "rootMap", null);

		// This should initialize the sorted map to have 15 items per block, sending key to the server,
		// and using the same namespace and set for the root map as the main key.
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(15, null, null, true);
		this.longMap = new SortedSubkeyMap<>(client, options);
		
		Key key = new Key("test", "Messages", "Tim");
		client.put(null, key, new Bin("Name", "Tim"), new Bin("Age", 111));

		for (long i = 1; i <= 20; i++) {
			this.longMap.put(key, i*1000, null, new Bin("Id", i*1000), new Bin("Message", "Message-" + i));
		}
		
		// The first key in the map should be 1000
		ResultsContainer<Long> results = this.longMap.getSubRecords(key, 0L, true, true, 1);
		Record[] records = results.getRecords();
		assertEquals(1, records.length);
		assertEquals(1000L, records[0].getLong("Id"));
		
		// Now delete the first record
		this.longMap.delete(key, 1000L, null);

		// The first key in the map should be 1000
		results = this.longMap.getSubRecords(key, 0L, true, true, 1);
		records = results.getRecords();
		assertEquals(1, records.length);
		assertEquals(2000L, records[0].getLong("Id"));
	}
	
	@Test
	public void testFirstKeyUpdate() {
		client.truncate(null, "test", "Messages", null);
		client.truncate(null, "test", "Messages-meta", null);
		client.truncate(null, "test", "Messages-subkeys", null);
		client.truncate(null, "test", "rootMap", null);

		// This should initialize the sorted map to have 15 items per block, sending key to the server,
		// and using the same namespace and set for the root map as the main key.
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(15, null, null, true);
		this.longMap = new SortedSubkeyMap<>(client, options);
		Key key = new Key("test", "Messages", "Updates");

		// Update the first key
		this.longMap.put(key, 1L, null, new Bin("test", "test"));
		this.longMap.put(key, 1L, null, new Bin("test", "test1"));
	}
	
	@Test
	public void testSecondKeyUpdate() {
		client.truncate(null, "test", "Messages", null);
		client.truncate(null, "test", "Messages-meta", null);
		client.truncate(null, "test", "Messages-subkeys", null);
		client.truncate(null, "test", "rootMap", null);

		// This should initialize the sorted map to have 15 items per block, sending key to the server,
		// and using the same namespace and set for the root map as the main key.
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(15, null, null, true);
		this.longMap = new SortedSubkeyMap<>(client, options);
		Key key = new Key("test", "Messages", "Updates");

		// Update the first key
		this.longMap.put(key, 1L, null, new Bin("test", "test"));
		this.longMap.put(key, 2L, null, new Bin("test", "test"));		
		this.longMap.put(key, 2L, null, new Bin("test", "test1"));
	}
	
}
