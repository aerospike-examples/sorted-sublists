package com.aerospike.storage.subkeys;

import com.aerospike.client.Key;

/**
 * Options for configuraing the SortedSubkeyMap. This class provides reasonable defaults for the options,
 * but can be overridden to customize them.
 * @author Tim
 *
 */
public class SortedSubkeyMapOptions {
	/** The namespace to use for the root block. If this is NULL, the namesapce containing the key for the operation will be used. */
	private String rootMapNamespace = "test";
	/** The set to use for the root block. If this is NULL, the set containing the key for the operation will be used. */
	private String rootMapSet = "rootMap";
	/** The bin to use for the root block. Cannot be null. */
	private String rootMapBin = "map";
	
	private String blockMapBin = "map";
	private String blockMapNextBin = "next";
	private String blockMapPrevBin = "prev";
	
	private long maxElementsPerBlock = 10000;
	private boolean sendKey = false;
	
	private int maxLockTimeMs = 100;
	
	public SortedSubkeyMapOptions() {
		super();
	}

	public SortedSubkeyMapOptions(boolean sendKey) {
		this.sendKey = sendKey;
	}

	public SortedSubkeyMapOptions(long maxElementsPerBlock) {
		this.maxElementsPerBlock = maxElementsPerBlock;
	}
	
	public SortedSubkeyMapOptions(long maxElementsPerBlock, boolean sendKey) {
		this(maxElementsPerBlock);
		this.sendKey = sendKey;
	}
	
	public SortedSubkeyMapOptions(long maxElementsPerBlock, String rootMapNamespace, String rootMapSet) {
		this(maxElementsPerBlock, rootMapNamespace, rootMapSet, false);
	}

	public SortedSubkeyMapOptions(long maxElementsPerBlock, String rootMapNamespace, String rootMapSet, boolean sendKey) {
		this.maxElementsPerBlock = maxElementsPerBlock;
		this.rootMapNamespace = rootMapNamespace;
		this.rootMapSet = rootMapSet;
		this.sendKey = sendKey;
	}

	public String getRootMapNamespace() {
		return rootMapNamespace;
	}

	public String getRootMapNamespace(Key key) {
		if (this.rootMapNamespace == null || this.rootMapNamespace.isEmpty()) {
			return key.namespace;
		}
		else {
			return this.rootMapNamespace;
		}
	}
	
	public void setRootMapNamespace(String rootMapNamespace) {
		this.rootMapNamespace = rootMapNamespace;
	}

	public String getRootMapSet() {
		return rootMapSet;
	}

	public String getRootMapSet(Key key) {
		if (this.rootMapSet == null || this.rootMapSet.isEmpty()) {
			return key.setName + SortedSubkeyMap.BLOCK_SETNAME_DIFFERENTIATOR;
		}
		else {
			return this.rootMapSet;
		}
	}
	
	public void setRootMapSet(String rootMapSet) {
		this.rootMapSet = rootMapSet;
	}

	public String getRootMapBin() {
		return rootMapBin;
	}

	public void setRootMapBin(String rootMapBin) {
		this.rootMapBin = rootMapBin;
	}

	public String getBlockMapBin() {
		return blockMapBin;
	}

	public void setBlockMapBin(String blockMapBin) {
		this.blockMapBin = blockMapBin;
	}

	public String getBlockMapNextBin() {
		return blockMapNextBin;
	}

	public void setBlockMapNextBin(String blockMapNextBin) {
		this.blockMapNextBin = blockMapNextBin;
	}

	public String getBlockMapPrevBin() {
		return blockMapPrevBin;
	}

	public void setBlockMapPrevBin(String blockMapPrevBin) {
		this.blockMapPrevBin = blockMapPrevBin;
	}

	public long getMaxElementsPerBlock() {
		return maxElementsPerBlock;
	}

	public void setMaxElementsPerBlock(long maxElementsPerBlock) {
		this.maxElementsPerBlock = maxElementsPerBlock;
	}
	
	public boolean isSendKey() {
		return sendKey;
	}
	
	public void setSendKey(boolean sendKey) {
		this.sendKey = sendKey;
	}
	
	public int getMaxLockTimeMs() {
		return maxLockTimeMs;
	}
	public void setMaxLockTimeMs(int maxLockTimeMs) {
		this.maxLockTimeMs = maxLockTimeMs;
	}
}
