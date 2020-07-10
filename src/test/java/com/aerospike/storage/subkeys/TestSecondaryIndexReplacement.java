package com.aerospike.storage.subkeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class TestSecondaryIndexReplacement {


	private final String HOST = "127.0.0.1";
	private AerospikeClient client;
	private static final String NAMESPACE = "test";
	private static final String USER_SET = "users";
	
	@Before
	public void setUp() throws Exception {
		 client = new AerospikeClient(HOST, 3000);
		client.truncate(null, NAMESPACE, USER_SET, null);
		client.truncate(null, NAMESPACE, USER_SET+"-meta", null);
		client.truncate(null, NAMESPACE, "rootMap", null);

	}

	@After
	public void tearDown() throws Exception {
		client.close();
	}
	
	public static class SegmentData {
		// Dummy data, doesn't really matter
		long timestamp;
		String source;
		
		public SegmentData(long timestamp, String source) {
			super();
			this.timestamp = timestamp;
			this.source = source;
		}

		public Value asValue() {
			List<Object> data = new ArrayList<>();
			data.add(timestamp);
			data.add(source);
			return Value.get(data);
		}
	}
	
	public class User {
		// assume the user name and email are unique for now.
		String name;
		String email;
		Map<String, SegmentData> data;
		
		User(String name, String email) {
			this.name = name;
			this.email = email;
			this.data = new HashMap<>();
		}
	}
	
	private void addSegmentToUser(User user, String segment, SegmentData data, ISortedSubkeyMap<String> sortedMap) {
		Key userKey = new Key(NAMESPACE, USER_SET, user.name);
		client.operate(null, userKey,
				Operation.put(new Bin("name", user.name)),
				Operation.put(new Bin("email", user.email)),
				MapOperation.put(new MapPolicy(MapOrder.KEY_ORDERED, 0), "segs", Value.get(segment), data.asValue()));
		
		sortedMap.put(new Key(NAMESPACE, USER_SET, segment), user.email, null, userKey);
	}
	@Test
	public void runTest() throws Exception {
		// Let's look at an ad-tech use case where we have structures like:
		// user -> { SEGMENT: { time_stamp, source } }
		// We want to get a list of the users who have a particular segment, sorted by time_stamp
		// In this case, the SEGMENT is unique and it's a logical record.
		// This could be done 
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(10, true);
		SortedSubkeyMap<String> map = new SortedSubkeyMap<>(client, options);

		User user = new User("Tim", "tim@aerospike.com");
		addSegmentToUser(user, "SPORTS", new SegmentData(new Date().getTime(), "internet"), map);
		addSegmentToUser(user, "ROBOTICS", new SegmentData(new Date().getTime(), "internet"), map);
		addSegmentToUser(user, "GARDENING", new SegmentData(new Date().getTime(), "satellite observation"), map);
		addSegmentToUser(user, "DOGS", new SegmentData(new Date().getTime(), "pet club membership"), map);
		addSegmentToUser(user, "COMPUTERS", new SegmentData(new Date().getTime(), "well, who doesn't?"), map);
		addSegmentToUser(user, "COOKING", new SegmentData(new Date().getTime(), "mans's gotta eat"), map);
		
		user = new User("Bob", "bob@aerospike.com");
		addSegmentToUser(user, "SPORTS", new SegmentData(new Date().getTime(), "internet"), map);
		addSegmentToUser(user, "GARDENING", new SegmentData(new Date().getTime(), "satellite observation"), map);
		addSegmentToUser(user, "COMPUTERS", new SegmentData(new Date().getTime(), "well, who doesn't?"), map);
		addSegmentToUser(user, "COOKING", new SegmentData(new Date().getTime(), "mans's gotta eat"), map);
		
		user = new User("Fred", "fred@aerospike.com");
		addSegmentToUser(user, "DOGS", new SegmentData(new Date().getTime(), "pet club membership"), map);
		addSegmentToUser(user, "COMPUTERS", new SegmentData(new Date().getTime(), "well, who doesn't?"), map);
		addSegmentToUser(user, "COOKING", new SegmentData(new Date().getTime(), "mans's gotta eat"), map);
		
		user = new User("John", "john@aerospike.com");
		addSegmentToUser(user, "SPORTS", new SegmentData(new Date().getTime(), "internet"), map);
		addSegmentToUser(user, "ROBOTICS", new SegmentData(new Date().getTime(), "internet"), map);
		addSegmentToUser(user, "GARDENING", new SegmentData(new Date().getTime(), "satellite observation"), map);
		addSegmentToUser(user, "DOGS", new SegmentData(new Date().getTime(), "pet club membership"), map);
		addSegmentToUser(user, "COOKING", new SegmentData(new Date().getTime(), "mans's gotta eat"), map);
		
		
		ResultsContainer<String> results = map.getSubRecords(new Key(NAMESPACE, USER_SET, "DOGS"), null, true, true, 100);
		assertEquals(3, results.getRecords().length);
	
		results = map.getSubRecords(new Key(NAMESPACE, USER_SET, "CATS"), null, true, true, 100);
		assertEquals(0, results.getRecords().length);

		results = map.getSubRecords(new Key(NAMESPACE, USER_SET, "COOKING"), null, true, true, 100);
		assertEquals(4, results.getRecords().length);
		for (int i = 0; i < results.getRecords().length-1; i++) {
			String thisName = results.getRecords()[i].getString("email");
			String nextName = results.getRecords()[i+1].getString("email");
			assertTrue(String.format("name %s is not less than %s", thisName, nextName), thisName.compareTo(nextName) < 0);
		}
	}
	
	@Test
	public void runLargeDataTest() throws Exception {
		// Create 10,000 users. Each user has 1-100 segments out of an available 200 segments. Find all users who have segment 100
	
		SortedSubkeyMapOptions options = new SortedSubkeyMapOptions(100, true);
		SortedSubkeyMap<String> map = new SortedSubkeyMap<>(client, options);

		// To verify the results, we will create a secondary index on the user table
		try {
			IndexTask task = client.createIndex(null, NAMESPACE, USER_SET, "usersIdx", "segs", IndexType.STRING, IndexCollectionType.MAPKEYS);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
		
		final int USER_COUNT = 10_00;
		final int MAX_USER_SEGMENTS = 100;
		final int NUM_SEGMENTS = 200;
		
		Random r = new Random();
		for (int userId = 0; userId < USER_COUNT; userId++) {
			
			int segmentsForThisUser = r.nextInt(MAX_USER_SEGMENTS)+1;
			for (int userSegId = 0; userSegId < segmentsForThisUser; userSegId++) {
				addSegmentToUser(new User("U"+userId, "U" + userId +"@nowhere.com"), "S" + r.nextInt(NUM_SEGMENTS), new SegmentData(new Date().getTime(), "data"), map);
			}
			System.out.printf("Created user %d\n", userId);
		}
		
		for (int i = 0; i < 50; i++) {
		long now = System.nanoTime();
		ResultsContainer<String> results = map.getSubRecords(new Key(NAMESPACE, USER_SET, "S100"), null, true, true, 10000);
		System.out.printf("Time: %.2fms\n", (System.nanoTime()-now)/1_000_000.0);
		}
		ResultsContainer<String> results = map.getSubRecords(new Key(NAMESPACE, USER_SET, "S100"), null, true, true, 10000);
		for (int i = 0; i < results.getRecords().length; i++) {
			System.out.println(results.getRecords()[i].getString("email"));
		}
		System.out.println(results.getRecords().length);
		
		Statement statement = new Statement();
		statement.setIndexName("usersIdx");
		statement.setNamespace(NAMESPACE);
		statement.setSetName(USER_SET);
		statement.setFilter(Filter.equal("segs", "S100"));
		RecordSet recordSet = client.query(null, statement);
		int count = 0;
		while (recordSet.next()) {
			count++;
		}
		recordSet.close();
		System.out.println(count);
		
		IndexTask task = client.dropIndex(null, NAMESPACE, USER_SET, "usersIdx");
		task.waitTillComplete();
		
	}
}
