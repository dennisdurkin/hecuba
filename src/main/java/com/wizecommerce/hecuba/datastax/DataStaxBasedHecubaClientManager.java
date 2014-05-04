package com.wizecommerce.hecuba.datastax;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ObjectArrays;
import com.wizecommerce.hecuba.*;

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
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteColumns(K key, List<String> columnNameList) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRow(K key) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRow(K key, long timestamp) {
		// TODO Auto-generated method stub

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
		String query = "select * from " + columnFamily + " where key in (" + StringUtils.repeat("?", ",", keys.size()) + ")";

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
		String query = "select * from " + columnFamily + " where key=? and column1 in (" + StringUtils.repeat("?", ",", columnNames.size()) + ")";

		CassandraResultSet<K, String> result = execute(query, ObjectArrays.concat(convertKey(key), columnNames.toArray(new Object[columnNames.size()])));

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception {
		String query = "select * from " + columnFamily + " where key in (" + StringUtils.repeat("?", ",", keys.size()) + ") and column1 in ("
				+ StringUtils.repeat("?", ",", columnNames.size()) + ")";

		columnNames.toArray();

		CassandraResultSet<K, String> result = execute(query, ObjectArrays.concat(convertKeys(keys), columnNames.toArray(new Object[columnNames.size()]), Object.class));

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
		String query = "select * from " + columnFamily + " where key in (" + StringUtils.repeat("?", ",", keys.size()) + ") and column1 >= ? and column1 <= ? limit ?";

		if (reversed) {
			throw new UnsupportedOperationException("Reversed is unsupported currently");
		}

		return execute(query, ObjectArrays.concat(convertKeys(keys), new Object[] { start, end, count }, Object.class));
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
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(K key, String columnName, String value, long timestamp, int ttl) {
		// TODO Auto-generated method stub

	}

	private Object convertKey(K key) {
		if (keyType == KeyType.LONG) {
			return key;
		}
		return key.toString();
	}

	private Object[] convertKeys(Collection<K> keys) {
		if (keyType == KeyType.LONG) {
			return keys.toArray(new Object[keys.size()]);
		}

		List<Object> convertedKeys = new ArrayList<>();

		for (K key : keys) {
			convertedKeys.add(key.toString());
		}

		return convertedKeys.toArray(new Object[convertedKeys.size()]);
	}

	private CassandraResultSet<K, String> execute(String query, Object... values) {
		logger.info("query = {}  values = {}", query, values);
		PreparedStatement stmt = session.prepare(query);

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

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
