# Sorted Sublists
## Concept
The Aerospike API contains a very flexible set of Complex Data Types (CDTs), embodied in Maps, Lists and Bitwise operations. Additionally, there are scan and query operations which allows entire sets to be returned, or records which match particular criteria. These can be combined with predicate expressions to return just records which match a relatively flexible criteria.

However, one feature which Aerospike does not support is sorting. Secondary index queries return the data in an ad-hoc fashion, and whilst the use of [PartitionFilters](https://www.aerospike.com/apidocs/java/index.html?com/aerospike/client/query/class-use/PartitionFilter.html) on scans allow repeatable, deterministic scans, these cannot be sorted by any stored key.

Sorting is expensive in a database like Aerospike due the cardinality of the data. Imagine you have a billion rows stored in a secondary index and needed them to be returned sorted. Obviously there is a computational cost of sorting this much data, but there is also a space cost -- if each record is 10k the raw data is about 10TB which makes it difficult to store even in temporary tables.

Sorting typically implies that the data is to be presented to a user so the user can scroll through to find the data relevant to them. This also implies pagination. There is no point retrieving 1,000,000 records in a data set when the user only wants to see 100 at a time.

This library presents an approach to have large collections of data sorted by a pre-defined key and the ability to paginate through them.

## Approach
Aerospike supports CDTs with powerful features. For this use case, a [key-sorted map](https://www.aerospike.com/docs/guide/cdt-map.html) becomes critical. 

Take for example, a database which stores users associated with a particular product. There could be thousands of such products, each with millions of users associated with them. The use case calls for the operator to be able to look at the users sorted by phone numbers  and be able to page forwards and backwards through them. Additionally, it is desired to be able to start pagination from an arbitrary point, for example starting from (500)000-0000 and getting entries from this point. 

(NB: This is obviously a hypothetical use case -- there are lots of privacy issues, GDPR issues, etc which are being glossed over)

 In this case, the key which we want to sort on and paginate through is the phone number of the user and it is assumed this key is unique.
 
 A first approach to solving this problem would be to store metadata about each record. This could be a map of phone numbers to the digests of the users, and stored as part of the product.
 
![DataModel](/images/DataModel.png)
 
 Since maps in Aerospike support sorting, this approach will work well -- we can store the metadata (phone number) as the key, and the digest of the record as the value. We can use map API operations to get the correct number of records (`getByIndexRange` or similar), and we can find the index of the first item to retrieve using `getByKeyRelativeIndexRange`. These API calls will give us the digests of the records which we need to retrieve, and a simple batch get will then fetch the actual records in the correct order, giving us the ability to return data which is sorted.
 
The problem with this approach is that the number of entries in a map are finite. Aerospike can store up to 8MB in a single record, and even though our entries are small (maybe 50 bytes each), there cannot be millions of entries in a sorted sub-list.

However, this approach could be scaled over several maps. Imagine our maps can only store 7 entries instead of thousands. 7 records are inserted into the map and prev and next pointers added, giving:

![DataModel](/images/DataModel1.png)

(Mapping to the user dropped for the sake of simplicity)

When the 8th item ((444)444-4444) is added, it's too big for the map to hold, so we need to split the map. We want to split the map equally, so in this case we would create another map referred to by the next pointer which contained the last 4 items, leaving the first 4 items in situ:

![DataModel](/images/DataModel2.png)

Note that in this use case it is assumed that the key (phone number) is unique -- adding a value with the same key will replace the existing key.

As more items are added to the sorted sub-list, the maps continue to split. The item is inserted into the block furthest along the chain which satisifes the condition of `new key >= smallest key in this block`. This criteria gives a very nice property: when a block splits we can always split the block that was selected for insertion into 2 sub-blocks without needing to change the next block. Consider adding more entries to the first block in the chain:

![DataModel](/images/DataModel3.png)

If we add (555)555-9000, this will still fall into the first block which will then have 8 entries and have to split, giving

![DataModel](/images/DataModel4.png)

Note that the block to the right (which starts with (555)555-9001) did not change at all, with the exception of where it's prev pointer is pointing.

This process of splitting the block where an item is being inserted allows a map to be formed which contains an unlimited number of entries. The items are sorted within a map, and the linked list of blocks are kept in sorted order, so we can traverse the list to retrieve items in sorted order.

Sometimes it is useful to be able to answer the query "what are the 3 next items starting from (500)000-000". This structure allows this query to be answered by searching each block in turn, but this is inefficient on very large sets of items. So some extra metadata can be added which keeps a list of the minimum value in each map:

![DataModel](/images/DataModel5.png)

This top level block is for speed and convenience only -- the same operations could be without it, but they would take significantly longer at scale. This top level block is typically kept in a memory-based namespace, as it can be rebuilt if it is lost.

## Secondary index replacement

This general concept can be used in some situations where secondary indexes are needed. A common ad-tech use case for example models Users (people using a browser and hence having ads shown to them) who have a list of Segments. A segment is an interest they have like BOOKS or SPORTS. When a bid request comes in, the ad-tech bidders look at the active campaigns they have and the interests they know this user has and determines the optimal price to bid to show an ad to that user. The data model often looks something like:

![DataModel](/images/AdTechModel.png)

This easily gives the answer to the common query "What segments does this user have?". However, this library also makes it possible to answer the question "What users possess a certain segment, sorted by user name?". It's a slightly different variation: the metadata pertains to a segment, not a user. So for each segment we will keep metadata, and the reference (digest) associated with the user name will be the digest of the user record. 

This would look like: 

![DataModel](/images/AdTechModel2.png)

Note in this case, each of the 4 segments would be a separate instance of a sorted sub-list, complete with prev/next pointers as per the discussion above. The only tricky thing is there are 2 keys defined, one for the segment (eg BOOKS) and one for the user data. There is a separate `put` method in the library which therefore takes 2 keys, and a test case provided. 

_**Note:**_ The above description details some of the internal workings of the library. It should not matter to its use. Also, there are a lot of details glossed over, like locking which prevents 2 threads inserting 2 different records in the same block simultaneous and both causing it to split.

Also note: Since this library requires metadata stored in a Map, it may not be suitable for Active/Active use cases for 2 distinct data centers joined with XDR, as 2 updates on both sides of the XDR connection can update the same set of metadata, causing metadata to be lost. If this is a requirement, please contact Client Services at Aerospike via support@aerospike.com.
