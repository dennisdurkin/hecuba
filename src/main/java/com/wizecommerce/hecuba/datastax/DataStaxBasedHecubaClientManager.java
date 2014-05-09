package com.wizecommerce.hecuba.datastax;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wizecommerce.hecuba.*;
import com.wizecommerce.hecuba.util.ClientManagerUtils;

/*
 * TODO: Exception throwing
 * TODO: Handle properties (timeouts, CL, etc...)
 */

public class DataStaxBasedHecubaClientManager<K> extends HecubaClientManager<K> {
	private static final Logger logger = LoggerFactory.getLogger(DataStaxBasedHecubaClientManager.class);

	private DataType keyType;
	private String datacenter;
	private String[] endpoints;
	private int port;
	private String keyspace;
	private String username;
	private String password;
	private ConsistencyLevel consistencyLevel;
	private LoadingCache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<String, PreparedStatement>() {
		@Override
		public PreparedStatement load(String query) throws Exception {
			PreparedStatement stmt = session.prepare(query);
			stmt.setConsistencyLevel(consistencyLevel);
			return stmt;
		}
	});

	private boolean compressionEnabled;

	private Session session;

	private String columnFamily;
	private String secondaryIndexColumnFamily;

	public DataStaxBasedHecubaClientManager(CassandraParamsBean parameters, DataType keyType) {
		super(parameters);

		this.keyType = keyType;

		this.datacenter = parameters.getDataCenter();
		this.endpoints = Iterables.toArray(Splitter.on(":").split(parameters.getLocationURLs()), String.class);
		this.keyspace = parameters.getKeyspace();
		this.columnFamily = '"' + parameters.getColumnFamily() + '"';
		// TODO: Get from configuration
		this.secondaryIndexColumnFamily = '"' + parameters.getColumnFamily() + HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX + '"';
		this.port = NumberUtils.toInt(parameters.getCqlPort());
		this.username = parameters.getUsername();
		this.password = parameters.getPassword();

		this.consistencyLevel = ConsistencyLevel.LOCAL_ONE;

		init();
	}

	@Override
	public void deleteColumn(K key, String columnName) {
		List<Object> values = new ArrayList<>();

		String query = "DELETE FROM " + columnFamily + " WHERE key = ? and column1 = ?";
		values.add(convertKey(key));
		values.add(columnName);

		execute(query, values.toArray());
	}

	@Override
	public void deleteColumns(K key, List<String> columnNames) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		if (columnNames.size() > 1) {
			builder.append("BEGIN UNLOGGED BATCH\n");
		}

		for (String columnName : columnNames) {
			builder.append("\tDELETE FROM " + columnFamily + " where key = ? and column1 = ?;\n");
			values.add(convertKey(key));
			values.add(columnName);
		}

		if (columnNames.size() > 1) {
			builder.append("APPLY BATCH;");
		}

		execute(builder.toString(), values.toArray());
	}

	@Override
	public void deleteRow(K key, long timestamp) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("DELETE FROM " + columnFamily);

		if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		}

		builder.append(" WHERE key = ?");
		values.add(convertKey(key));

		execute(builder.toString(), values.toArray());
	}

	@Override
	public Long getCounterValue(K key, String counterColumnName) {
		String query = "select * from " + columnFamily + " where key=? and column1=?";

		CassandraResultSet<K, String> result = execute(query, null, null, ImmutableMap.of(counterColumnName, DataType.counter()), convertKey(key), counterColumnName);

		if (result.hasResults()) {
			return result.getLong(counterColumnName);
		}

		return null;
	}

	@Override
	public void updateCounter(K key, String counterColumnName, long value) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("UPDATE " + columnFamily + "set value = value + ? where key = ? and column1 = ?");
		values.add(value);
		values.add(convertKey(key));
		values.add(counterColumnName);

		execute(builder.toString(), values.toArray());
	}

	@Override
	public void incrementCounter(K key, String counterColumnName) {
		updateCounter(key, counterColumnName, 1);
	}

	@Override
	public void decrementCounter(K key, String counterColumnName) {
		updateCounter(key, counterColumnName, -1);
	}

	@Override
	public CassandraResultSet<K, String> readAllColumns(K key) throws Exception {
		String query = "select * from " + columnFamily + " where key=?";

		return execute(query, convertKey(key));
	}

	@Override
	public CassandraResultSet<K, String> readAllColumns(Set<K> keys) throws Exception {
		String query = "select * from " + columnFamily + " where key in ?";

		return execute(query, convertKeys(keys));

	}

	@Override
	public CassandraResultSet readAllColumnsBySecondaryIndex(Map<String, String> parameters, int limit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CassandraColumn readColumnInfo(K key, String columnName) {
		String query = "select key, column1, value, writetime(value), ttl(value) from " + columnFamily + " where key=? and column1=?";

		PreparedStatement stmt = statementCache.getUnchecked(query);

		BoundStatement bind = stmt.bind(convertKey(key), columnName);
		ResultSet rs = session.execute(bind);

		Iterator<Row> iterator = rs.iterator();
		if (iterator.hasNext()) {
			Row row = iterator.next();
			long timestamp = row.getLong("writetime(value)");
			int ttl = row.getInt("ttl(value)");
			String value = row.getString("value");
			return new CassandraColumn(columnName, value, timestamp, ttl);
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(K key, List<String> columnNames) throws Exception {
		if (CollectionUtils.isEmpty(columnNames)) {
			return readAllColumns(key);
		}

		String query = "select * from " + columnFamily + " where key=? and column1 in ?";

		CassandraResultSet<K, String> result = execute(query, convertKey(key), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception {
		if (CollectionUtils.isEmpty(columnNames)) {
			return readAllColumns(keys);
		}

		String query = "select * from " + columnFamily + " where key in ? and column1 in ?";

		CassandraResultSet<K, String> result = execute(query, convertKeys(keys), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(K key, String start, String end, boolean reversed, int count) {
		List<Object> values = new ArrayList<>();
		StringBuilder builder = new StringBuilder();
		builder.append("select * from " + columnFamily + " where key=?");
		values.add(convertKey(key));

		if (start != null) {
			builder.append(" and column1 >= ?");
			values.add(start);
		}

		if (end != null) {
			builder.append(" and column1 <= ?");
			values.add(end);
		}

		if (reversed) {
			builder.append(" order by column1 desc");
		}

		if (count > 0) {
			builder.append(" limit ?");
			values.add(count);
		}

		return execute(builder.toString(), values.toArray());
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(Set<K> keys, String start, String end, boolean reversed, int count) {
		List<Object> values = new ArrayList<>();
		StringBuilder builder = new StringBuilder();
		builder.append("select * from " + columnFamily + " where key in ?");
		values.add(convertKeys(keys));

		if (start != null) {
			builder.append(" and column1 >= ?");
			values.add(start);
		}

		if (end != null) {
			builder.append(" and column1 <= ?");
			values.add(end);
		}

		if (reversed) {
			builder.append(" order by column1 desc");
		}

		if (count > 0) {
			// TODO: Need to limit count per key, not overall count...is that even possible with CQL?
			builder.append(" limit ?");
			values.add(count);
		}

		return execute(builder.toString(), values.toArray());
	}

	@Override
	public String readString(K key, String columnName) {
		String query = "select * from " + columnFamily + " where key=? and column1=?";

		CassandraResultSet<K, String> result = execute(query, convertKey(key), columnName);

		if (result.hasResults()) {
			return result.getString(columnName);
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveByColumnNameBasedSecondaryIndex(String columnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, List<String> columnValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, String columnValue) {
		String query = "select * from " + secondaryIndexColumnFamily + " where key = ?";
		CassandraResultSet<K, String> keysResultSet = execute(query, DataType.ascii(), keyType, ImmutableMap.of("*", keyType), getSecondaryIndexKey(columnName, columnValue));
		Set<K> keys = new LinkedHashSet<>();
		if (keysResultSet.hasResults()) {
			for (String key : keysResultSet.getColumnNames()) {
				if (keyType == DataType.bigint()) {
					keys.add((K) NumberUtils.createLong(key));
				} else {
					keys.add((K) key);
				}
			}

			try {
				return readAllColumns(keys);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		return null;
	}

	@Override
	public List<K> retrieveKeysByColumnNameBasedSecondaryIndex(String columnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<K>> retrieveKeysBySecondaryIndex(String columnName, List<String> columnValues) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<K> retrieveKeysBySecondaryIndex(String columnName, String columnValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateByteBuffer(K key, String columnName, ByteBuffer value) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("INSERT INTO " + columnFamily + "(key, column1, value) values (?,?,?)");
		values.add(convertKey(key));
		values.add(columnName);
		values.add(value);

		execute(builder.toString(), values.toArray());
	}

	@Override
	public void updateRow(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) throws Exception {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		updateSecondaryIndexes(key, row, timestamps, ttls);

		for (Map.Entry<String, Object> entry : row.entrySet()) {
			builder.append("\tINSERT INTO " + columnFamily + " (key, column1, value) values (?,?,?)");
			values.add(convertKey(key));
			values.add(entry.getKey());
			String valueToInsert = ClientManagerUtils.getInstance().convertValueForStorage(entry.getValue());
			values.add(valueToInsert);

			Long timestamp = timestamps != null ? timestamps.get(entry.getKey()) : null;
			Integer ttl = ttls != null ? ttls.get(entry.getKey()) : null;

			if (timestamp != null && timestamp > 0 && ttl != null && ttl > 0) {
				builder.append(" USING TIMESTAMP ? and TTL ?");
				values.add(timestamp);
				values.add(ttl);
			} else if (timestamp != null && timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			} else if (ttl != null && ttl > 0) {
				builder.append(" USING TTL ?");
				values.add(ttl);
			}

			builder.append(";\n");
		}

		builder.append("APPLY BATCH;");

		execute(builder.toString(), values.toArray());
	}

	@Override
	public void updateString(K key, String columnName, String value, long timestamp, int ttl) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		updateSecondaryIndexes(key, columnName, value, timestamp, ttl);

		builder.append("INSERT INTO " + columnFamily + "(key, column1, value) values (?,?,?)");
		values.add(convertKey(key));
		values.add(columnName);
		values.add(value);

		if (timestamp > 0 && ttl > 0) {
			builder.append(" USING TIMESTAMP ? and TTL ?");
			values.add(timestamp);
			values.add(ttl);
		} else if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		} else if (ttl > 0) {
			builder.append(" USING TTL ?");
			values.add(ttl);
		}

		execute(builder.toString(), values.toArray());
	}

	private void updateSecondaryIndexes(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) {
		List<String> secondaryColumnsChanged = null;
		List<String> secondaryIndexByColumnNameChanges = null;

		// Gather list of secondary index columns that are being changed
		for (Map.Entry<String, Object> entry : row.entrySet()) {
			String column = entry.getKey();
			if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(column)) {
				if (secondaryColumnsChanged == null) {
					// Lazy initialize to prevent wasting memory
					secondaryColumnsChanged = new ArrayList<>();
				}
				secondaryColumnsChanged.add(column);
			}
			if (isSecondaryIndexByColumnNameEnabledForColumn(column)) {
				if (secondaryIndexByColumnNameChanges == null) {
					// Lazy initialize to prevent wasting memory
					secondaryIndexByColumnNameChanges = new ArrayList<>();
				}
				secondaryIndexByColumnNameChanges.add(column);
			}
		}

		if (CollectionUtils.isNotEmpty(secondaryColumnsChanged)) {
			Map<String, String> oldValues = new HashMap<>();
			try {
				CassandraResultSet<K, String> readColumns = readColumns(key, secondaryColumnsChanged);
				for (String column : readColumns.getColumnNames()) {
					oldValues.put(column, readColumns.getString(column));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			updateSecondaryIndexes(key, row, timestamps, ttls, secondaryColumnsChanged, oldValues);
		}

		if (CollectionUtils.isNotEmpty(secondaryIndexByColumnNameChanges)) {
			updateSecondaryIndexes(key, null, timestamps, ttls, secondaryIndexByColumnNameChanges, null);
		}
	}

	private void updateSecondaryIndexes(K key, String columnName, String value, long timestamp, int ttl) {
		if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(columnName)) {
			String oldValue = readString(key, columnName);
			updateSecondaryIndexes(key, columnName, value, timestamp, ttl, oldValue);
		}

		if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
			updateSecondaryIndexes(key, columnName, "", timestamp, ttl, "");
		}
	}

	private void updateSecondaryIndexes(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls, List<String> columnsChanged,
			Map<String, String> oldValues) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		if (columnsChanged != null) {
			for (String columnName : columnsChanged) {
				String oldValue = oldValues.get(columnName);
				if (!StringUtils.isBlank(oldValue) && !"null".equalsIgnoreCase(oldValue)) {
					// Delete old value if there is one (if it's null we'll just be writing it again down below with updated TS and TTL)
					builder.append("\tDELETE FROM " + secondaryIndexColumnFamily);

					Long timestamp = timestamps != null ? timestamps.get(columnName) : null;
					if (timestamp != null && timestamp > 0) {
						builder.append(" USING TIMESTAMP ?");
						values.add(timestamp);
					}

					builder.append(" where key = ? and column1 = ?;\n");
					values.add(getSecondaryIndexKey(columnName, oldValue));
					values.add(convertKey(key));
				}
			}
		}

		for (String columnName : columnsChanged) {
			Object value = row.get(columnName);
			String valueToInsert = ClientManagerUtils.getInstance().convertValueForStorage(value);

			// Insert New Value
			builder.append("\tINSERT INTO " + secondaryIndexColumnFamily + "(key, column1, value) values (?,?,?)");
			values.add(getSecondaryIndexKey(columnName, valueToInsert));
			values.add(convertKey(key));
			values.add(convertKey(key));

			Long timestamp = timestamps != null ? timestamps.get(columnName) : null;
			Integer ttl = ttls != null ? ttls.get(columnName) : null;
			if (timestamp != null && timestamp > 0 && ttl != null && ttl > 0) {
				builder.append(" USING TIMESTAMP ? and TTL ?");
				values.add(timestamp);
				values.add(ttl);
			} else if (timestamp != null && timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			} else if (ttl != null && ttl > 0) {
				builder.append(" USING TTL ?");
				values.add(ttl);
			}
			builder.append(";\n");
		}

		builder.append("APPLY BATCH;");

		execute(builder.toString(), values.toArray());
	}

	private void updateSecondaryIndexes(K key, String columnName, String value, long timestamp, int ttl, String oldValue) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		if (!StringUtils.isBlank(oldValue) && !"null".equalsIgnoreCase(oldValue)) {
			// Delete old value if there is one (if it's null we'll just be writing it again down below with updated TS and TTL)
			builder.append("\tDELETE FROM " + secondaryIndexColumnFamily);

			if (timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			}

			builder.append(" where key = ? and column1 = ?;\n");
			values.add(getSecondaryIndexKey(columnName, oldValue));
			values.add(convertKey(key));
		}

		// Insert New Value
		builder.append("\tINSERT INTO " + secondaryIndexColumnFamily + "(key, column1, value) values (?,?,?)");
		values.add(getSecondaryIndexKey(columnName, value));
		values.add(convertKey(key));
		values.add(convertKey(key));

		if (timestamp > 0 && ttl > 0) {
			builder.append(" USING TIMESTAMP ? and TTL ?");
			values.add(timestamp);
			values.add(ttl);
		} else if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		} else if (ttl > 0) {
			builder.append(" USING TTL ?");
			values.add(ttl);
		}
		builder.append(";\n");

		builder.append("APPLY BATCH;");

		execute(builder.toString(), values.toArray());
	}

	private Object convertKey(K key) {
		if (keyType.getName() == Name.BIGINT) {
			return key;
		} else if (keyType.getName() == Name.TEXT) {
			return key.toString();
		} else {
			throw new RuntimeException("Unhandled DataType: " + keyType);
		}
	}

	private List<?> convertKeys(Set<K> keys) {
		if (keyType.getName() == Name.BIGINT) {
			return new ArrayList<>(keys);
		} else if (keyType.getName() != Name.TEXT) {
			throw new RuntimeException("Unhandled DataType: " + keyType);
		}

		List<Object> convertedKeys = new ArrayList<>(keys.size());

		for (K key : keys) {
			convertedKeys.add(key.toString());
		}

		return convertedKeys;
	}

	private CassandraResultSet<K, String> execute(String query, Object... values) {
		return execute(query, null, null, null, values);
	}

	private CassandraResultSet<K, String> execute(String query, DataType keyType, DataType columnType, Map<String, DataType> valueTypes, Object... values) {
		logger.debug("query = {} : values = {}", query, values);
		PreparedStatement stmt = statementCache.getUnchecked(query);

		BoundStatement bind = stmt.bind(values);
		long startTimeNanos = System.nanoTime();
		ResultSet rs = session.execute(bind);
		long durationNanos = System.nanoTime() - startTimeNanos;

		DataStaxCassandraResultSet<K> cassandraResultSet = new DataStaxCassandraResultSet<>(rs, ObjectUtils.defaultIfNull(keyType, this.keyType), columnType, valueTypes,
				durationNanos);

		return cassandraResultSet;
	}

	private void init() {
		LoadBalancingPolicy loadBalancingPolicy;
		if (datacenter != null) {
			loadBalancingPolicy = new DCAwareRoundRobinPolicy(datacenter);
		} else {
			loadBalancingPolicy = new RoundRobinPolicy();
		}
		loadBalancingPolicy = new TokenAwarePolicy(loadBalancingPolicy);
		loadBalancingPolicy = LatencyAwarePolicy.builder(loadBalancingPolicy).build();

		Builder builder = Cluster.builder().addContactPoints(endpoints).withLoadBalancingPolicy(loadBalancingPolicy);

		if (port > 0) {
			builder.withPort(port);
		}

		if (username != null && password != null) {
			builder.withCredentials(username, password);
		}

		if (compressionEnabled) {
			builder.withCompression(Compression.LZ4);
		}

		Cluster cluster = builder.build();
		session = cluster.connect('"' + keyspace + '"');
	}

	@Override
	protected void logDownedHosts() {
	}
}
