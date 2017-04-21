package com.microsoft.azure.elasticdb.shard.mapmanager.unittests;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryBehavior;
import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.ShardKeyType;
import com.microsoft.azure.elasticdb.shard.cache.CacheStore;
import com.microsoft.azure.elasticdb.shard.map.ListShardMap;
import com.microsoft.azure.elasticdb.shard.map.RangeShardMap;
import com.microsoft.azure.elasticdb.shard.map.ShardMap;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementException;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerFactory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerLoadPolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.category.ExcludeFromGatedCheckin;
import com.microsoft.azure.elasticdb.shard.mapmanager.decorators.CountingCacheStore;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlStoreConnectionFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * 
 * Test related to ShardMapManager class and it's methods.
 *
 */
public class ShardMapManagerTests {

	// Shard map name used in the tests.
	private static String s_shardMapName = "Customer";

	/**
	 * Initializes common state for tests in this class.
	 * 
	 * @throws SQLServerException
	 */
	@BeforeClass
	public static void shardMapManagerTestsInitialize() throws SQLServerException {
		SQLServerConnection conn = null;
		try {
			conn = (SQLServerConnection) DriverManager.getConnection(Globals.ShardMapManagerTestConnectionString);
			try (Statement stmt = conn.createStatement()) {
				// Create ShardMapManager database
				String query = String.format(Globals.CreateDatabaseQuery, Globals.ShardMapManagerDatabaseName);
				stmt.executeUpdate(query);
			} catch (SQLException ex) {
				ex.printStackTrace();
			}

			// Create the shard map manager.
			ShardMapManagerFactory.CreateSqlShardMapManager(Globals.ShardMapManagerConnectionString,
					ShardMapManagerCreateMode.ReplaceExisting);
		} catch (Exception e) {
			System.out.printf("Failed to connect to SQL database with connection string:", e.getMessage());
		} finally {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}

	/**
	 * Cleans up common state for the all tests in this class.
	 * 
	 * @throws SQLServerException
	 */
	@AfterClass
	public static void shardMapManagerTestsCleanup() throws SQLServerException {
		SQLServerConnection conn = null;
		try {
			conn = (SQLServerConnection) DriverManager.getConnection(Globals.ShardMapManagerTestConnectionString);
			// Create ShardMapManager database
			try (Statement stmt = conn.createStatement()) {
				String query = String.format(Globals.DropDatabaseQuery, Globals.ShardMapManagerDatabaseName);
				stmt.executeUpdate(query);
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		} catch (Exception e) {
			System.out.printf("Failed to connect to SQL database with connection string:", e.getMessage());
		} finally {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		}
	}

	/**
	 * Initializes common state per-test.
	 */
	@Before
	public void shardMapManagerTestInitialize() {
		ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString,
				ShardMapManagerLoadPolicy.Lazy);
		try {
			ShardMap sm = smm.GetShardMap(ShardMapManagerTests.s_shardMapName);
			smm.DeleteShardMap(sm);
		} catch (ShardManagementException smme) {
			assertTrue(smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure);
		}
	}

	/**
	 * Cleans up common state per-test.
	 */
	@After
	public void shardMapManagerTestCleanup() {
		ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString,
				ShardMapManagerLoadPolicy.Lazy);
		try {
			ShardMap sm = smm.GetShardMap(ShardMapManagerTests.s_shardMapName);
			smm.DeleteShardMap(sm);
		} catch (ShardManagementException smme) {
			assertTrue(smme.getErrorCode() == ShardManagementErrorCode.ShardMapLookupFailure);
		}
	}

	/**
	 * Create list shard map.
	 * 
	 * @throws Exception
	 */
	@Test
	@Category(value = ExcludeFromGatedCheckin.class)
	public void createListShardMapDefault() throws Exception {
		CountingCacheStore cacheStore = new CountingCacheStore(new CacheStore());

		ShardMapManager smm = new ShardMapManager(
				new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
				new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
				ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
				RetryBehavior.getDefaultRetryBehavior());

		ListShardMap<Integer> lsm = smm.CreateListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

		assertNotNull(lsm);

		ShardMap smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);
		assertNotNull(smLookup);
		assertEquals(ShardMapManagerTests.s_shardMapName, smLookup.getName());
		assertEquals(1, cacheStore.getLookupShardMapCount());
		assertEquals(1, cacheStore.getLookupShardMapHitCount());
	}

	/**
	 * Create range shard map.
	 * 
	 * @throws Exception
	 */
	@Test
	@Category(value = ExcludeFromGatedCheckin.class)
	public void createRangeShardMapDefault() throws Exception {
		CountingCacheStore cacheStore = new CountingCacheStore(new CacheStore());

		ShardMapManager smm = new ShardMapManager(
				new SqlShardMapManagerCredentials(Globals.ShardMapManagerConnectionString),
				new SqlStoreConnectionFactory(), new StoreOperationFactory(), cacheStore,
				ShardMapManagerLoadPolicy.Lazy, RetryPolicy.DefaultRetryPolicy,
				RetryBehavior.getDefaultRetryBehavior());

		RangeShardMap<Integer> rsm = smm.CreateRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

		assertNotNull(rsm);
		assertEquals(ShardMapManagerTests.s_shardMapName, rsm.getName());

		ShardMap smLookup = smm.LookupShardMapByName("LookupShardMapByName", ShardMapManagerTests.s_shardMapName, true);

		assertNotNull(smLookup);
		assertEquals(ShardMapManagerTests.s_shardMapName, smLookup.getName());
		assertEquals(1, cacheStore.getLookupShardMapCount());
		assertEquals(1, cacheStore.getLookupShardMapHitCount());

	}

	/**
	 * Add a list shard map with duplicate name to shard map manager.
	 * 
	 * @throws Exception
	 */
	@Test
	@Category(value = ExcludeFromGatedCheckin.class)
	public void CreateListShardMapDuplicate() throws Exception {
		ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString,
				ShardMapManagerLoadPolicy.Lazy);

		ShardMap sm = smm.CreateListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

		assertNotNull(sm);

		assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

		boolean creationFailed = false;

		try {
			ListShardMap<Integer> lsm = smm.CreateListShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);

		} catch (ShardManagementException sme) {
			assertEquals(ShardManagementErrorCategory.ShardMapManager, sme.getErrorCategory());
			assertEquals(ShardManagementErrorCode.ShardAlreadyExists, sme.getErrorCode());
			creationFailed = true;
		}
		assertTrue(creationFailed);
	}

	/**
	 * Add a range shard map with duplicate name to shard map manager.
	 * 
	 * @throws Exception
	 */
	@Test
	@Category(value = ExcludeFromGatedCheckin.class)
	public void CreateRangeShardMapDuplicate() throws Exception {
		ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(Globals.ShardMapManagerConnectionString,
				ShardMapManagerLoadPolicy.Lazy);
		ShardMap sm = smm.CreateRangeShardMap(ShardMapManagerTests.s_shardMapName, ShardKeyType.Int32);
		assertNotNull(sm);

		assertEquals(ShardMapManagerTests.s_shardMapName, sm.getName());

		boolean creationFailed = false;

		try {
			RangeShardMap<Integer> rsm = smm.CreateRangeShardMap(ShardMapManagerTests.s_shardMapName,
					ShardKeyType.Int32);

		} catch (ShardManagementException sme) {
			assertEquals(ShardManagementErrorCategory.ShardMapManager, sme.getErrorCategory());
			assertEquals(ShardManagementErrorCode.ShardAlreadyExists, sme.getErrorCode());
			creationFailed = true;
		}
		assertTrue(creationFailed);
	}

}
