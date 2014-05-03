package com.wizecommerce.hecuba.datastax;

import java.net.InetAddress;
import java.util.*;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.wizecommerce.hecuba.AbstractCassandraResultSet;
import com.wizecommerce.hecuba.datastax.DataStaxBasedHecubaClientManager.KeyType;

public class DataStaxCassandraResultSet<K> extends AbstractCassandraResultSet<K, String> {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private ResultSet rs;
	private Iterator<Row> rowIterator;
	private KeyType keyType;
	private Map<String, String> currentRow = new LinkedHashMap<>();
	private Map<String, String> nextRow = new LinkedHashMap<>();
	private K currentKey;
	private K nextKey;
	private boolean firstTime = true;
	private long durationNanos;

	public DataStaxCassandraResultSet(ResultSet rs, KeyType keyType, long durationNanos) {
		this.rs = rs;
		this.rowIterator = rs.iterator();
		this.durationNanos = durationNanos;
		this.keyType = keyType;
	}

	@SuppressWarnings("unchecked")
	private void extractRow() {
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			K key = (K) (keyType == KeyType.LONG ? row.getLong("key") : row.getString("key"));
			if (currentKey == null) {
				currentKey = key;
			} else if (!Objects.equal(key, currentKey)) {
				nextKey = key;

				String column = row.getString("column1");
				String value = row.getString("value");
				nextRow.put(column, value);

				break;
			}
			String column = row.getString("column1");
			String value = row.getString("value");
			currentRow.put(column, value);
		}
	}

	public boolean hasNext() {
		if (!firstTime) {
			return nextKey != null;
		}
		if (currentKey == null) {
			extractRow();
		}
		return currentKey != null;
	}

	public void next() {
		if (!firstTime) {
			currentKey = nextKey;
			currentRow = nextRow;

			nextKey = null;
			nextRow = new LinkedHashMap<>();

			extractRow();
		} else {
			firstTime = false;
		}
	}

	@Override
	public boolean hasNextResult() {
		return hasNext();
	}

	@Override
	public void nextResult() {
		next();
	}

	@Override
	public String getHost() {
		ExecutionInfo executionInfo = rs.getExecutionInfo();
		Host queriedHost = executionInfo.getQueriedHost();
		InetAddress address = queriedHost.getAddress();
		return address.getCanonicalHostName();
	}

	@Override
	public long getExecutionLatency() {
		return durationNanos;
	}

	@Override
	public K getKey() {
		return currentKey;
	}

	@Override
	public UUID getUUID(String columnName) {
		return null;
	}

	@Override
	public String getString(String columnName) {
		return currentRow.get(columnName);
	}

	@Override
	public byte[] getByteArray(String columnName) {
		return currentRow.get(columnName).getBytes();
	}

	@Override
	public Collection<String> getColumnNames() {
		return currentRow.keySet();
	}

	@Override
	public boolean hasResults() {
		return hasNext();
	}

	@Override
	public String toString() {
		Map<K, Map<String, String>> allResults = new LinkedHashMap<>();

		while (hasNext()) {
			next();
			allResults.put(currentKey, currentRow);
		}

		return gson.toJson(allResults);
	}
}
