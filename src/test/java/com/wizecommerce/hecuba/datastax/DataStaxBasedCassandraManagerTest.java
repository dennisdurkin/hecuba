package com.wizecommerce.hecuba.datastax;

import java.io.IOException;

import org.mortbay.util.MultiMap;

import com.datastax.driver.core.DataType;
import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.HecubaCassandraManagerTestBase;
import com.wizecommerce.hecuba.HecubaClientManager;

public class DataStaxBasedCassandraManagerTest extends HecubaCassandraManagerTestBase {

	public DataStaxBasedCassandraManagerTest() throws IOException {
		super(DataStaxBasedCassandraManagerTest.class.getName());
	}

	@Override
	protected MultiMap getData(String columnFamilyName) {
		return null;
	}

	@Override
	protected void tearDown() {

	}

	public HecubaClientManager<Long> getHecubaClientManager(CassandraParamsBean params) {
		return new DataStaxBasedHecubaClientManager<>(params, DataType.bigint());
	}

}
