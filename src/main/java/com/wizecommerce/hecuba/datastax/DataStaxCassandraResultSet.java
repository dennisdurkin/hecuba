package com.wizecommerce.hecuba.datastax;

import java.net.InetAddress;
import java.util.*;

import com.datastax.driver.core.*;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.wizecommerce.hecuba.AbstractCassandraResultSet;

public class DataStaxCassandraResultSet<K> extends AbstractCassandraResultSet<K, String> {
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	private ResultSet rs;
	private Iterator<Row> rowIterator;
	private DataType keyType;
	private Map<String, DataType> valueTypes = new HashMap<>();
	private Map<String, Object> currentRow = new LinkedHashMap<>();
	private Map<String, Object> nextRow = new LinkedHashMap<>();
	private K currentKey;
	private K nextKey;
	private boolean firstTime = true;
	private long durationNanos;

	public DataStaxCassandraResultSet(ResultSet rs, DataType keyType, Map<String, DataType> valueTypes, long durationNanos) {
		this.rs = rs;
		this.rowIterator = rs.iterator();
		this.durationNanos = durationNanos;
		this.keyType = keyType;
		this.valueTypes = valueTypes;
	}

	@SuppressWarnings("unchecked")
	private void extractRow() {
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			K key = (K) getValue(row, "key", keyType);
			if (currentKey == null) {
				currentKey = key;
			} else if (!Objects.equal(key, currentKey)) {
				nextKey = key;

				String column = row.getString("column1");
				DataType valueType = valueTypes != null ? valueTypes.get(column) : null;
				Object value = getValue(row, "value", valueType);
				nextRow.put(column, value);

				break;
			}
			String column = row.getString("column1");
			Object value = row.getString("value");
			currentRow.put(column, value);
		}
	}

	private Object getValue(Row row, String column, DataType dataType) {
		if (dataType == null) {
			return row.getString(column);
		}

		switch (dataType.getName()) {
		case ASCII:
		case VARCHAR:
		case TEXT:
			return row.getString(column);
		case BIGINT:
		case COUNTER:
			return row.getLong(column);
		case BLOB:
			return row.getBytes(column);
		case BOOLEAN:
			return row.getBool(column);
		case DECIMAL:
			return row.getDecimal(column);
		case DOUBLE:
			return row.getDouble(column);
		case FLOAT:
			return row.getFloat(column);
		case INET:
			return row.getInet(column);
		case TIMESTAMP:
		case INT:
			return row.getInt(column);
		case TIMEUUID:
		case UUID:
			return row.getUUID(column);
		case VARINT:
			return row.getVarint(column);
		case CUSTOM:
			return row.getBytesUnsafe(column);
		default:
			throw new RuntimeException("Unhandled DataType: " + dataType);
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
		return (UUID) currentRow.get(columnName);
	}

	@Override
	public String getString(String columnName) {
		Object value = currentRow.get(columnName);
		if (value != null) {
			return value.toString();
		}
		return null;
	}

	@Override
	public byte[] getByteArray(String columnName) {
		String value = getString(columnName);
		if (value != null) {
			return value.getBytes();
		}
		return null;
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
		Map<K, Map<String, Object>> allResults = new LinkedHashMap<>();

		while (hasNext()) {
			next();
			allResults.put(currentKey, currentRow);
		}

		return gson.toJson(allResults);
	}
}
