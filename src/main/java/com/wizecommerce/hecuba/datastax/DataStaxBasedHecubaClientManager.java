package com.wizecommerce.hecuba.datastax;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.wizecommerce.hecuba.*;
import com.wizecommerce.hecuba.util.ClientManagerUtils;

public class DataStaxBasedHecubaClientManager<K> extends HecubaClientManager<K> {
	private static final Logger logger = LoggerFactory.getLogger(DataStaxBasedHecubaClientManager.class);

	public static enum KeyType {
		STRING, LONG
	};

	private KeyType keyType;
	private String datacenter;
	private String[] endpoints;
	private int port;
	private String keyspace;
	private String username;
	private String password;
	private ConsistencyLevel consistencyLevel;

	private boolean compressionEnabled;

	private Session session;

	private String columnFamily;

	public DataStaxBasedHecubaClientManager(CassandraParamsBean parameters, KeyType keyType) {
		super(parameters);

		this.keyType = keyType;

		this.datacenter = parameters.getDataCenter();
		this.endpoints = Iterables.toArray(Splitter.on(":").split(parameters.getLocationURLs()), String.class);
		this.keyspace = parameters.getKeyspace();
		this.columnFamily = '"' + parameters.getColumnFamily() + '"';
		this.port = NumberUtils.toInt(parameters.getCqlPort());
		this.username = parameters.getUsername();
		this.password = parameters.getPassword();

		this.consistencyLevel = ConsistencyLevel.LOCAL_ONE;

		init();
	}

	@Override
	public void addColumnFamily(String keyspace, String columnFamilyName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void createKeyspace(String keyspace) {
		// TODO Auto-generated method stub

	}

	@Override
	public void createKeyspaceAndColumnFamilies(String keyspace, List<ColumnFamilyInfo> columnFamilies) {
		// TODO Auto-generated method stub

	}

	@Override
	public void decrementCounter(K key, String counterColumnName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteColumn(K key, String columnName) {
		List<Object> values = new ArrayList<>();

		String query = "DELETE FROM " + columnFamily + " WHERE key = ? and column1 = ?";
		values.add(convertKey(key));
		values.add(columnName);

		execute(query, values.toArray(new Object[values.size()]));
	}

	@Override
	public void deleteColumns(K key, List<String> columnNames) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		if (columnNames.size() > 1) {
			builder.append("BEGIN BATCH\n");
		}

		for (String columnName : columnNames) {
			builder.append("\tDELETE FROM " + columnFamily + " where key = ? and column1 = ?;\n");
			values.add(convertKey(key));
			values.add(columnName);
		}

		if (columnNames.size() > 1) {
			builder.append("APPLY BATCH;");
		}

		execute(builder.toString(), values.toArray(new Object[values.size()]));
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

		execute(builder.toString(), values.toArray(new Object[values.size()]));
	}

	@Override
	public void dropColumnFamily(String keyspace, String columnFamilyName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void dropKeyspace(String keyspace) {
		// TODO Auto-generated method stub

	}

	@Override
	public Long getCounterValue(K key, String counterColumnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void incrementCounter(K key, String counterColumnName) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(K key, List<String> columnNames) throws Exception {
		String query = "select * from " + columnFamily + " where key=? and column1 in ?";

		CassandraResultSet<K, String> result = execute(query, convertKey(key), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception {
		String query = "select * from " + columnFamily + " where key in ? and column1 in ?";

		CassandraResultSet<K, String> result = execute(query, convertKeys(keys), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(K key, String start, String end, boolean reversed, int count) {
		String query = "select * from " + columnFamily + " where key=? and column1 >= ? and column1 <= ? limit ?";

		if (reversed) {
			throw new UnsupportedOperationException("Reversed is unsupported currently");
		}

		return execute(query, convertKey(key), start, end, count);
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(Set<K> keys, String start, String end, boolean reversed, int count) {
		String query = "select * from " + columnFamily + " where key in ? and column1 >= ? and column1 <= ? limit ?";

		if (reversed) {
			throw new UnsupportedOperationException("Reversed is unsupported currently");
		}

		return execute(query, convertKeys(keys), start, end, count);
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

	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, String columnValue) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCounter(K key, String counterColumnName, long value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRow(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) throws Exception {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		if (row.size() > 1) {
			builder.append("BEGIN BATCH\n");
		}

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

		if (row.size() > 1) {
			builder.append("APPLY BATCH;");
		}

		execute(builder.toString(), values.toArray(new Object[values.size()]));
	}

	@Override
	public void updateString(K key, String columnName, String value, long timestamp, int ttl) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

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

		execute(builder.toString(), values.toArray(new Object[values.size()]));
	}

	private Object convertKey(K key) {
		if (keyType == KeyType.LONG) {
			return key;
		}
		return key.toString();
	}

	private List<?> convertKeys(Set<K> keys) {
		if (keyType == KeyType.LONG) {
			return new ArrayList<>(keys);
		}

		List<Object> convertedKeys = new ArrayList<>(keys.size());

		for (K key : keys) {
			convertedKeys.add(key.toString());
		}

		return convertedKeys;
	}

	private CassandraResultSet<K, String> execute(String query, Object... values) {
		logger.info("query = {} : values = {}", query, values);
		PreparedStatement stmt = session.prepare(query);
		stmt.setConsistencyLevel(consistencyLevel);

		BoundStatement bind = stmt.bind(values);
		long startTimeNanos = System.nanoTime();
		ResultSet rs = session.execute(bind);
		long durationNanos = System.nanoTime() - startTimeNanos;

		DataStaxCassandraResultSet<K> cassandraResultSet = new DataStaxCassandraResultSet<>(rs, keyType, durationNanos);

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

	public static void main(String[] args) throws Exception {
		try {
			BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n")));

			CassandraParamsBean parameters = new CassandraParamsBean();

			parameters.setColumnFamily("PTitle");
			parameters.setDataCenter("DC1");
			parameters.setLocationURLs("v-cass1.nextagqa.com");
			parameters.setKeyspace("NextagTest");

			DataStaxBasedHecubaClientManager<Long> client = new DataStaxBasedHecubaClientManager<>(parameters, KeyType.LONG);

			CassandraResultSet<Long, String> readAllColumns;

			readAllColumns = client.readAllColumns(135573L);
			System.out.println("readAllColumns = " + readAllColumns);

			readAllColumns = client.readAllColumns(ImmutableSet.of(135573L, 135585L));
			System.out.println("readAllColumns = " + readAllColumns);

			String readString = client.readString(135573L, "SOURCE_ID");
			System.out.println("readString = " + readString);

			readAllColumns = client.readColumns(135573L, Arrays.asList("SOURCE_ID", "ID", "CATEGORY_ID"));
			System.out.println("readColumns = " + readAllColumns);

			readAllColumns = client.readColumns(ImmutableSet.of(135573L, 135585L), Arrays.asList("SOURCE_ID", "ID", "CATEGORY_ID"));
			System.out.println("readColumns = " + readAllColumns);

			readAllColumns = client.readColumnSlice(135573L, "A", "D", false, 100);
			System.out.println("readColumnSlice = " + readAllColumns);

			readAllColumns = client.readColumnSlice(ImmutableSet.of(135573L, 135585L), "D", "O", false, 100);
			System.out.println("readColumnSlice = " + readAllColumns);

			client.updateString(135573L, "DUMMY_COLUMN", DateTime.now().toString(), -1, 1000);
			readString = client.readString(135573L, "DUMMY_COLUMN");
			System.out.println("readString = " + readString);

			client.updateRow(135585L, ImmutableMap.of("DUMMY_COLUMN2", (Object) DateTime.now().toString(), "DUMMY_COLUMN3", DateTime.now().toString()),
					ImmutableMap.of("DUMMY_COLUMN2", -1L), ImmutableMap.of("DUMMY_COLUMN3", 30));
			readAllColumns = client.readAllColumns(135585L);
			System.out.println("readAllColumns = " + readAllColumns);

			client.deleteColumn(135573L, "DUMMY_COLUMN");
			readString = client.readString(135573L, "DUMMY_COLUMN");
			System.out.println("readString = " + readString);

			client.deleteColumns(135585L, Arrays.asList("DUMMY_COLUMN2", "DUMMY_COLUMN3"));
			readAllColumns = client.readAllColumns(135585L);
			System.out.println("readAllColumns = " + readAllColumns);

			client.deleteColumns(135585L, Arrays.asList("DUMMY_COLUMN2", "DUMMY_COLUMN3"));
			readAllColumns = client.readAllColumns(135585L);
			System.out.println("readAllColumns = " + readAllColumns);

			client.deleteRow(122741L, DateTime.now().getMillis() * 1000);
			readAllColumns = client.readAllColumns(115533L);
			System.out.println("readAllColumns = " + readAllColumns);

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
