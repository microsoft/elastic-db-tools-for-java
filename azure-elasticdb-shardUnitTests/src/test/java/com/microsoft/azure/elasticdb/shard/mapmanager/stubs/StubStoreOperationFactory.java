package com.microsoft.azure.elasticdb.shard.mapmanager.stubs;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.RetryPolicy;
import com.microsoft.azure.elasticdb.shard.base.LockOwnerIdOpType;
import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.cache.CacheStoreMappingUpdatePolicy;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardManagementErrorCategory;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManager;
import com.microsoft.azure.elasticdb.shard.mapmanager.ShardMapManagerCreateMode;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func2Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func3Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func4Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func5Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func6Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func7Param;
import com.microsoft.azure.elasticdb.shard.mapmanager.stubhelper.Func8Param;
import com.microsoft.azure.elasticdb.shard.sqlstore.SqlShardMapManagerCredentials;
import com.microsoft.azure.elasticdb.shard.store.StoreLogEntry;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperation;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationGlobal;
import com.microsoft.azure.elasticdb.shard.storeops.base.IStoreOperationLocal;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationCode;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationFactory;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationState;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.JAXBElement;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Stub type of Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.StoreOperationFactory
 */
public class StubStoreOperationFactory extends StoreOperationFactory {

  /**
   * Sets the stub of StoreOperationFactory.createAddMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mapping)
   */
  public Func4Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, IStoreOperation> CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping;
  /**
   * Sets the stub of StoreOperationFactory.createAddMappingOperation(StoreOperationCode
   * operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState
   * undoStartState, JAXBElement root, Guid originalShardVersionAdds)
   */
  public Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> CreateAddMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
  /**
   * Sets the stub of StoreOperationFactory.createAddShardMapGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap)
   */
  public Func3Param<ShardMapManager, String, StoreShardMap, IStoreOperationGlobal> CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
  /**
   * Sets the stub of StoreOperationFactory.createAddShardOperation(ShardMapManager shardMapManager,
   * Guid operationId, StoreOperationState undoStartState, JAXBElement root)
   */
  public Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> CreateAddShardOperationShardMapManagerGuidStoreOperationStateXElement;
  /**
   * Sets the stub of StoreOperationFactory.createAddShardOperation(ShardMapManager shardMapManager,
   * StoreShardMap shardMap, StoreShard shard)
   */
  public Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard;
  /**
   * Sets the stub of StoreOperationFactory.createAddShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreSchemaInfo schemaInfo)
   */
  public Func3Param<ShardMapManager, String, StoreSchemaInfo, IStoreOperationGlobal> CreateAddShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
  /**
   * Sets the stub of StoreOperationFactory.createAttachShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shard)
   */
  public Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> CreateAttachShardOperationShardMapManagerIStoreShardMapIStoreShard;
  /**
   * Sets the stub of StoreOperationFactory.createCheckShardLocalOperation(String operationName,
   * ShardMapManager shardMapManager, ShardLocation location)
   */
  public Func3Param<String, ShardMapManager, ShardLocation, IStoreOperationLocal> CreateCheckShardLocalOperationStringShardMapManagerShardLocation;
  /**
   * Sets the stub of StoreOperationFactory.createCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials
   * credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode
   * createMode, Version targetVersion)
   */
  public Func5Param<SqlShardMapManagerCredentials, RetryPolicy, String, ShardMapManagerCreateMode, Version, IStoreOperationGlobal> CreateCreateShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringShardMapManagerCreateModeVersion;
  /**
   * Sets the stub of StoreOperationFactory.createDetachShardGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, ShardLocation location, String shardMapName)
   */
  public Func4Param<ShardMapManager, String, ShardLocation, String, IStoreOperationGlobal> CreateDetachShardGlobalOperationShardMapManagerStringShardLocationString;
  /**
   * Sets the stub of StoreOperationFactory.createFindMappingByIdGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping,
   * ShardManagementErrorCategory errorCategory)
   */
  public Func5Param<ShardMapManager, String, StoreShardMap, StoreMapping, ShardManagementErrorCategory, IStoreOperationGlobal> CreateFindMappingByIdGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingShardManagementErrorCategory;
  /**
   * Sets the stub of StoreOperationFactory.createFindMappingByKeyGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, ShardKey key,
   * CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, Boolean
   * cacheResults, Boolean ignoreFailure)
   */
  public Func8Param<ShardMapManager, String, StoreShardMap, ShardKey, CacheStoreMappingUpdatePolicy, ShardManagementErrorCategory, Boolean, Boolean, IStoreOperationGlobal> CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean;
  /**
   * Sets the stub of StoreOperationFactory.createFindShardByLocationGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, ShardLocation location)
   */
  public Func4Param<ShardMapManager, String, StoreShardMap, ShardLocation, IStoreOperationGlobal> CreateFindShardByLocationGlobalOperationShardMapManagerStringIStoreShardMapShardLocation;
  /**
   * Sets the stub of StoreOperationFactory.createFindShardMapByNameGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String shardMapName)
   */
  public Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> CreateFindShardMapByNameGlobalOperationShardMapManagerStringString;
  /**
   * Sets the stub of StoreOperationFactory.createFindShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String schemaInfoName)
   */
  public Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> CreateFindShardingSchemaInfoGlobalOperationShardMapManagerStringString;
  /**
   * Sets the stub of StoreOperationFactory.createGetDistinctShardLocationsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  public Func2Param<ShardMapManager, String, IStoreOperationGlobal> CreateGetDistinctShardLocationsGlobalOperationShardMapManagerString;
  /**
   * Sets the stub of StoreOperationFactory.createGetMappingsByRangeGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange
   * range, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean
   * ignoreFailure)
   */
  public Func8Param<ShardMapManager, String, StoreShardMap, StoreShard, ShardRange, ShardManagementErrorCategory, Boolean, Boolean, IStoreOperationGlobal> CreateGetMappingsByRangeGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardShardRangeShardManagementErrorCategoryBooleanBoolean;
  /**
   * Sets the stub of StoreOperationFactory.createGetMappingsByRangeLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap,
   * StoreShard shard, ShardRange range, Boolean ignoreFailure)
   */
  public Func7Param<ShardMapManager, ShardLocation, String, StoreShardMap, StoreShard, ShardRange, Boolean, IStoreOperationLocal> CreateGetMappingsByRangeLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardShardRangeBoolean;
  /**
   * Sets the stub of StoreOperationFactory.createGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials
   * credentials, RetryPolicy retryPolicy, String operationName, Boolean throwOnFailure)
   */
  public Func4Param<SqlShardMapManagerCredentials, RetryPolicy, String, Boolean, IStoreOperationGlobal> CreateGetShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringBoolean;
  /**
   * Sets the stub of StoreOperationFactory.createGetShardMapsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  public Func2Param<ShardMapManager, String, IStoreOperationGlobal> CreateGetShardMapsGlobalOperationShardMapManagerString;
  /**
   * Sets the stub of StoreOperationFactory.createGetShardingSchemaInfosGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  public Func2Param<ShardMapManager, String, IStoreOperationGlobal> CreateGetShardingSchemaInfosGlobalOperationShardMapManagerString;
  /**
   * Sets the stub of StoreOperationFactory.createGetShardsGlobalOperation(String operationName,
   * ShardMapManager shardMapManager, StoreShardMap shardMap)
   */
  public Func3Param<String, ShardMapManager, StoreShardMap, IStoreOperationGlobal> CreateGetShardsGlobalOperationStringShardMapManagerIStoreShardMap;
  /**
   * Sets the stub of StoreOperationFactory.createGetShardsLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName)
   */
  public Func3Param<ShardMapManager, ShardLocation, String, IStoreOperationLocal> CreateGetShardsLocalOperationShardMapManagerShardLocationString;
  /**
   * Sets the stub of StoreOperationFactory.createLoadShardMapManagerGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  public Func2Param<ShardMapManager, String, IStoreOperationGlobal> CreateLoadShardMapManagerGlobalOperationShardMapManagerString;
  /**
   * Sets the stub of StoreOperationFactory.createLockOrUnLockMappingsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping, Guid
   * lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
   */
  public Func7Param<ShardMapManager, String, StoreShardMap, StoreMapping, UUID, LockOwnerIdOpType, ShardManagementErrorCategory, IStoreOperationGlobal> CreateLockOrUnLockMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingGuidLockOwnerIdOpTypeShardManagementErrorCategory;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mapping, Guid lockOwnerId)
   */
  public Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, UUID, IStoreOperation> CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveMappingOperation(StoreOperationCode
   * operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState
   * undoStartState, JAXBElement root, Guid originalShardVersionRemoves)
   */
  public Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> CreateRemoveMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardMapGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap)
   */
  public Func3Param<ShardMapManager, String, StoreShardMap, IStoreOperationGlobal> CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardOperation(ShardMapManager
   * shardMapManager, Guid operationId, StoreOperationState undoStartState, JAXBElement root)
   */
  public Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> CreateRemoveShardOperationShardMapManagerGuidStoreOperationStateXElement;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shard)
   */
  public Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard;
  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String schemaInfoName)
   */
  public Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> CreateRemoveShardingSchemaInfoGlobalOperationShardMapManagerStringString;
  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard,
   * IEnumerable`1&lt;StoreMapping&gt; mappingsToRemove, IEnumerable`1&lt;StoreMapping&gt;
   * mappingsToAdd)
   */
  public Func6Param<ShardMapManager, String, StoreShardMap, StoreShard, List<StoreMapping>, List<StoreMapping>, IStoreOperationGlobal> CreateReplaceMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardIEnumerableOfIStoreMappingIEnumerableOfIStoreMapping;
  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap,
   * StoreShard shard, IEnumerable`1&lt;ShardRange&gt; rangesToRemove,
   * IEnumerable`1&lt;StoreMapping&gt; mappingsToAdd)
   */
  public Func7Param<ShardMapManager, ShardLocation, String, StoreShardMap, StoreShard, List<ShardRange>, List<StoreMapping>, IStoreOperationLocal> CreateReplaceMappingsLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardIEnumerableOfShardRangeIEnumerableOfIStoreMapping;
  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap,
   * Pair`2&lt;StoreMapping,Guid&gt;[] mappingsSource, Pair`2&lt;StoreMapping,Guid&gt;[]
   * mappingsTarget)
   */
  public Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap, List<Pair<StoreMapping, UUID>>, List<Pair<StoreMapping, UUID>>, IStoreOperation> CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray;
  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsOperation(StoreOperationCode
   * operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState
   * undoStartState, JAXBElement root, Guid originalShardVersionAdds)
   */
  public Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> CreateReplaceMappingsOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid;
  /**
   * Sets the stub of StoreOperationFactory.createUpdateMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mappingSource, StoreMapping mappingTarget, String patternForKill, Guid lockOwnerId)
   */
  public Func7Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, StoreMapping, String, UUID, IStoreOperation> CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuid;
  /**
   * Sets the stub of StoreOperationFactory.createUpdateMappingOperation(StoreOperationCode
   * operationCode, ShardMapManager shardMapManager, Guid operationId, StoreOperationState
   * undoStartState, JAXBElement root, Guid originalShardVersionRemoves, Guid
   * originalShardVersionAdds)
   */
  public Func7Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, UUID, IStoreOperation> CreateUpdateMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuidGuid;
  /**
   * Sets the stub of StoreOperationFactory.createUpdateShardOperation(ShardMapManager
   * shardMapManager, Guid operationId, StoreOperationState undoStartState, JAXBElement root)
   */
  public Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> CreateUpdateShardOperationShardMapManagerGuidStoreOperationStateXElement;
  /**
   * Sets the stub of StoreOperationFactory.createUpdateShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew)
   */
  public Func4Param<ShardMapManager, StoreShardMap, StoreShard, StoreShard, IStoreOperation> CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard;
  /**
   * Sets the stub of StoreOperationFactory.createUpdateShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreSchemaInfo schemaInfo)
   */
  public Func3Param<ShardMapManager, String, StoreSchemaInfo, IStoreOperationGlobal> CreateUpdateShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo;
  /**
   * Sets the stub of StoreOperationFactory.createUpgradeStoreGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, Version targetVersion)
   */
  public Func3Param<ShardMapManager, String, Version, IStoreOperationGlobal> CreateUpgradeStoreGlobalOperationShardMapManagerStringVersion;
  /**
   * Sets the stub of StoreOperationFactory.createUpgradeStoreLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, Version targetVersion)
   */
  public Func4Param<ShardMapManager, ShardLocation, String, Version, IStoreOperationLocal> CreateUpgradeStoreLocalOperationShardMapManagerShardLocationStringVersion;
  /**
   * Sets the stub of StoreOperationFactory.fromLogEntry(ShardMapManager shardMapManager,
   * StoreLogEntry so)
   */
  public Func2Param<ShardMapManager, StoreLogEntry, IStoreOperation> FromLogEntryShardMapManagerIStoreLogEntry;
  private boolean ___callBase;
  private IStubBehavior ___instanceBehavior;

  /**
   * Initializes a new instance
   */
  public StubStoreOperationFactory() {
    this.InitializeStub();
  }

  /**
   * Gets or sets a value that indicates if the base method should be called instead of the fallback
   * behavior
   */
  public final boolean getCallBase() {
    return this.___callBase;
  }

  public final void setCallBase(boolean value) {
    this.___callBase = value;
  }

  /**
   * Gets or sets the instance behavior.
   */
  public final IStubBehavior getInstanceBehavior() {
    return StubBehaviors.GetValueOrCurrent(this.___instanceBehavior);
  }

  public final void setInstanceBehavior(IStubBehavior value) {
    this.___instanceBehavior = value;
  }

  /**
   * Sets the stub of StoreOperationFactory.createAddMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mapping)
   */
  @Override
  public IStoreOperation createAddMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping) {
    Func4Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, IStoreOperation> func1 = (ShardMapManager arg1, StoreOperationCode arg2, StoreShardMap arg3, StoreMapping arg4) -> CreateAddMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMapping
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationCode, shardMap, mapping);
    }
    if (this.___callBase) {
      return super.createAddMappingOperation(shardMapManager, operationCode, shardMap, mapping);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddMappingOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createAddMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      JAXBElement root, UUID originalShardVersionAdds) {
    Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> func1 = (StoreOperationCode arg1, ShardMapManager arg2, UUID arg3, StoreOperationState arg4, JAXBElement arg5, UUID arg6) -> CreateAddMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6);
    if (func1 != null) {
      return func1.invoke(operationCode, shardMapManager, operationId, undoStartState, root,
          originalShardVersionAdds);
    }
    if (this.___callBase) {
      return super
          .createAddMappingOperation(operationCode, shardMapManager, operationId, undoStartState,
              root, originalShardVersionAdds);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddMappingOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createAddShardMapGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap)
   */
  @Override
  public IStoreOperationGlobal createAddShardMapGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap) {
    Func3Param<ShardMapManager, String, StoreShardMap, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3) -> CreateAddShardMapGlobalOperationShardMapManagerStringIStoreShardMap
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap);
    }
    if (this.___callBase) {
      return super.createAddShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddShardMapGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createAddShardOperation(ShardMapManager shardMapManager,
   * StoreShardMap shardMap, StoreShard shard)
   */
  @Override
  public IStoreOperation createAddShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard) {
    Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> func1 = (ShardMapManager arg1, StoreShardMap arg2, StoreShard arg3) -> CreateAddShardOperationShardMapManagerIStoreShardMapIStoreShard
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, shardMap, shard);
    }
    if (this.___callBase) {
      return super.createAddShardOperation(shardMapManager, shardMap, shard);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddShardOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createAddShardOperation(ShardMapManager shardMapManager, UUID operationId,
      StoreOperationState undoStartState, JAXBElement root) {
    Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> func1 = (ShardMapManager arg1, UUID arg2, StoreOperationState arg3, JAXBElement arg4) -> CreateAddShardOperationShardMapManagerGuidStoreOperationStateXElement
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationId, undoStartState, root);
    }
    if (this.___callBase) {
      return super.createAddShardOperation(shardMapManager, operationId, undoStartState, root);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddShardOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createAddShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreSchemaInfo schemaInfo)
   */
  @Override
  public IStoreOperationGlobal createAddShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreSchemaInfo schemaInfo) {
    Func3Param<ShardMapManager, String, StoreSchemaInfo, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreSchemaInfo arg3) -> CreateAddShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, schemaInfo);
    }
    if (this.___callBase) {
      return super
          .createAddShardingSchemaInfoGlobalOperation(shardMapManager, operationName, schemaInfo);
    }
    return this.getInstanceBehavior().Result(this,
        "createAddShardingSchemaInfoGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createAttachShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shard)
   */
  @Override
  public IStoreOperation createAttachShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard) {
    Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> func1 = (ShardMapManager arg1, StoreShardMap arg2, StoreShard arg3) -> CreateAttachShardOperationShardMapManagerIStoreShardMapIStoreShard
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, shardMap, shard);
    }
    if (this.___callBase) {
      return super.createAttachShardOperation(shardMapManager, shardMap, shard);
    }
    return this.getInstanceBehavior().Result(this,
        "createAttachShardOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createCheckShardLocalOperation(String operationName,
   * ShardMapManager shardMapManager, ShardLocation location)
   */
  @Override
  public IStoreOperationLocal createCheckShardLocalOperation(String operationName,
      ShardMapManager shardMapManager, ShardLocation location) {
    Func3Param<String, ShardMapManager, ShardLocation, IStoreOperationLocal> func1 = (String arg1, ShardMapManager arg2, ShardLocation arg3) -> CreateCheckShardLocalOperationStringShardMapManagerShardLocation
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(operationName, shardMapManager, location);
    }
    if (this.___callBase) {
      return super.createCheckShardLocalOperation(operationName, shardMapManager, location);
    }
    return this.getInstanceBehavior().Result(this,
        "createCheckShardLocalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createCreateShardMapManagerGlobalOperation(SqlShardMapManagerCredentials
   * credentials, RetryPolicy retryPolicy, String operationName, ShardMapManagerCreateMode
   * createMode, Version targetVersion)
   */
  @Override
  public IStoreOperationGlobal createCreateShardMapManagerGlobalOperation(
      SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy,
      String operationName, ShardMapManagerCreateMode createMode, Version targetVersion) {
    Func5Param<SqlShardMapManagerCredentials, RetryPolicy, String, ShardMapManagerCreateMode, Version, IStoreOperationGlobal> func1 = (SqlShardMapManagerCredentials arg1, RetryPolicy arg2, String arg3, ShardMapManagerCreateMode arg4, Version arg5) -> CreateCreateShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringShardMapManagerCreateModeVersion
        .invoke(arg1, arg2, arg3, arg4, arg5);
    if (func1 != null) {
      return func1.invoke(credentials, retryPolicy, operationName, createMode, targetVersion);
    }
    if (this.___callBase) {
      return super
          .createCreateShardMapManagerGlobalOperation(credentials, retryPolicy, operationName,
              createMode, targetVersion);
    }
    return this.getInstanceBehavior().Result(this,
        "createCreateShardMapManagerGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createDetachShardGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, ShardLocation location, String shardMapName)
   */
  @Override
  public IStoreOperationGlobal createDetachShardGlobalOperation(ShardMapManager shardMapManager,
      String operationName, ShardLocation location, String shardMapName) {
    Func4Param<ShardMapManager, String, ShardLocation, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, ShardLocation arg3, String arg4) -> CreateDetachShardGlobalOperationShardMapManagerStringShardLocationString
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, location, shardMapName);
    }
    if (this.___callBase) {
      return super
          .createDetachShardGlobalOperation(shardMapManager, operationName, location, shardMapName);
    }
    return this.getInstanceBehavior().Result(this,
        "createDetachShardGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createFindMappingByIdGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping,
   * ShardManagementErrorCategory errorCategory)
   */
  @Override
  public IStoreOperationGlobal createFindMappingByIdGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreMapping mapping,
      ShardManagementErrorCategory errorCategory) {
    Func5Param<ShardMapManager, String, StoreShardMap, StoreMapping, ShardManagementErrorCategory, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, StoreMapping arg4, ShardManagementErrorCategory arg5) -> CreateFindMappingByIdGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingShardManagementErrorCategory
        .invoke(arg1, arg2, arg3, arg4, arg5);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap, mapping, errorCategory);
    }
    if (this.___callBase) {
      return super
          .createFindMappingByIdGlobalOperation(shardMapManager, operationName, shardMap, mapping,
              errorCategory);
    }
    return this.getInstanceBehavior().Result(this,
        "createFindMappingByIdGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createFindMappingByKeyGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, ShardKey key,
   * CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory, Boolean
   * cacheResults, Boolean ignoreFailure)
   */
  @Override
  public IStoreOperationGlobal createFindMappingByKeyGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap, ShardKey key,
      CacheStoreMappingUpdatePolicy policy, ShardManagementErrorCategory errorCategory,
      boolean cacheResults, boolean ignoreFailure) {
    Func8Param<ShardMapManager, String, StoreShardMap, ShardKey, CacheStoreMappingUpdatePolicy, ShardManagementErrorCategory, Boolean, Boolean, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, ShardKey arg4, CacheStoreMappingUpdatePolicy arg5, ShardManagementErrorCategory arg6, Boolean arg7, Boolean arg8) -> CreateFindMappingByKeyGlobalOperationShardMapManagerStringIStoreShardMapShardKeyCacheStoreMappingUpdatePolicyShardManagementErrorCategoryBooleanBoolean
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap, key, policy, errorCategory,
          cacheResults, ignoreFailure);
    }
    if (this.___callBase) {
      return super
          .createFindMappingByKeyGlobalOperation(shardMapManager, operationName, shardMap, key,
              policy, errorCategory, cacheResults, ignoreFailure);
    }
    return this.getInstanceBehavior().Result(this,
        "createFindMappingByKeyGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createFindShardByLocationGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, ShardLocation location)
   */
  @Override
  public IStoreOperationGlobal createFindShardByLocationGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap,
      ShardLocation location) {
    Func4Param<ShardMapManager, String, StoreShardMap, ShardLocation, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, ShardLocation arg4) -> CreateFindShardByLocationGlobalOperationShardMapManagerStringIStoreShardMapShardLocation
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap, location);
    }
    if (this.___callBase) {
      return super
          .createFindShardByLocationGlobalOperation(shardMapManager, operationName, shardMap,
              location);
    }
    return this.getInstanceBehavior().Result(this,
        "createFindShardByLocationGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createFindShardMapByNameGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String shardMapName)
   */
  @Override
  public IStoreOperationGlobal createFindShardMapByNameGlobalOperation(
      ShardMapManager shardMapManager, String operationName, String shardMapName) {
    Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, String arg3) -> CreateFindShardMapByNameGlobalOperationShardMapManagerStringString
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMapName);
    }
    if (this.___callBase) {
      return super
          .createFindShardMapByNameGlobalOperation(shardMapManager, operationName, shardMapName);
    }
    return this.getInstanceBehavior().Result(this,
        "createFindShardMapByNameGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createFindShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String schemaInfoName)
   */
  @Override
  public IStoreOperationGlobal createFindShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, String schemaInfoName) {
    Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, String arg3) -> CreateFindShardingSchemaInfoGlobalOperationShardMapManagerStringString
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, schemaInfoName);
    }
    if (this.___callBase) {
      return super.createFindShardingSchemaInfoGlobalOperation(shardMapManager, operationName,
          schemaInfoName);
    }
    return this.getInstanceBehavior().Result(this,
        "createFindShardingSchemaInfoGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetDistinctShardLocationsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  @Override
  public IStoreOperationGlobal createGetDistinctShardLocationsGlobalOperation(
      ShardMapManager shardMapManager, String operationName) {
    Func2Param<ShardMapManager, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2) -> CreateGetDistinctShardLocationsGlobalOperationShardMapManagerString
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName);
    }
    if (this.___callBase) {
      return super.createGetDistinctShardLocationsGlobalOperation(shardMapManager, operationName);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetDistinctShardLocationsGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetMappingsByRangeGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard, ShardRange
   * range, ShardManagementErrorCategory errorCategory, Boolean cacheResults, Boolean
   * ignoreFailure)
   */
  @Override
  public IStoreOperationGlobal createGetMappingsByRangeGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap,
      StoreShard shard, ShardRange range, ShardManagementErrorCategory errorCategory,
      boolean cacheResults, boolean ignoreFailure) {
    Func8Param<ShardMapManager, String, StoreShardMap, StoreShard, ShardRange, ShardManagementErrorCategory, Boolean, Boolean, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, StoreShard arg4, ShardRange arg5, ShardManagementErrorCategory arg6, Boolean arg7, Boolean arg8) -> CreateGetMappingsByRangeGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardShardRangeShardManagementErrorCategoryBooleanBoolean
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap, shard, range, errorCategory,
          cacheResults, ignoreFailure);
    }
    if (this.___callBase) {
      return super
          .createGetMappingsByRangeGlobalOperation(shardMapManager, operationName, shardMap, shard,
              range, errorCategory, cacheResults, ignoreFailure);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetMappingsByRangeGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetMappingsByRangeLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap,
   * StoreShard shard, ShardRange range, Boolean ignoreFailure)
   */
  @Override
  public IStoreOperationLocal createGetMappingsByRangeLocalOperation(
      ShardMapManager shardMapManager, ShardLocation location, String operationName,
      StoreShardMap shardMap, StoreShard shard, ShardRange range, boolean ignoreFailure) {
    Func7Param<ShardMapManager, ShardLocation, String, StoreShardMap, StoreShard, ShardRange, Boolean, IStoreOperationLocal> func1 = (ShardMapManager arg1, ShardLocation arg2, String arg3, StoreShardMap arg4, StoreShard arg5, ShardRange arg6, Boolean arg7) -> CreateGetMappingsByRangeLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardShardRangeBoolean
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    if (func1 != null) {
      return func1
          .invoke(shardMapManager, location, operationName, shardMap, shard, range, ignoreFailure);
    }
    if (this.___callBase) {
      return super.createGetMappingsByRangeLocalOperation(shardMapManager, location, operationName,
          shardMap, shard, range, ignoreFailure);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetMappingsByRangeLocalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetShardMapManagerGlobalOperation(SqlShardMapManagerCredentials
   * credentials, RetryPolicy retryPolicy, String operationName, Boolean throwOnFailure)
   */
  @Override
  public IStoreOperationGlobal createGetShardMapManagerGlobalOperation(
      SqlShardMapManagerCredentials credentials, RetryPolicy retryPolicy,
      String operationName, boolean throwOnFailure) {
    Func4Param<SqlShardMapManagerCredentials, RetryPolicy, String, Boolean, IStoreOperationGlobal> func1 = (SqlShardMapManagerCredentials arg1, RetryPolicy arg2, String arg3, Boolean arg4) -> CreateGetShardMapManagerGlobalOperationSqlShardMapManagerCredentialsTransientFaultHandlingRetryPolicyStringBoolean
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(credentials, retryPolicy, operationName, throwOnFailure);
    }
    if (this.___callBase) {
      return super.createGetShardMapManagerGlobalOperation(credentials, retryPolicy, operationName,
          throwOnFailure);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetShardMapManagerGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetShardMapsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  @Override
  public IStoreOperationGlobal createGetShardMapsGlobalOperation(ShardMapManager shardMapManager,
      String operationName) {
    Func2Param<ShardMapManager, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2) -> CreateGetShardMapsGlobalOperationShardMapManagerString
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName);
    }
    if (this.___callBase) {
      return super.createGetShardMapsGlobalOperation(shardMapManager, operationName);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetShardMapsGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetShardingSchemaInfosGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  @Override
  public IStoreOperationGlobal createGetShardingSchemaInfosGlobalOperation(
      ShardMapManager shardMapManager, String operationName) {
    Func2Param<ShardMapManager, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2) -> CreateGetShardingSchemaInfosGlobalOperationShardMapManagerString
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName);
    }
    if (this.___callBase) {
      return super.createGetShardingSchemaInfosGlobalOperation(shardMapManager, operationName);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetShardingSchemaInfosGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetShardsGlobalOperation(String operationName,
   * ShardMapManager shardMapManager, StoreShardMap shardMap)
   */
  @Override
  public IStoreOperationGlobal createGetShardsGlobalOperation(String operationName,
      ShardMapManager shardMapManager, StoreShardMap shardMap) {
    Func3Param<String, ShardMapManager, StoreShardMap, IStoreOperationGlobal> func1 = (String arg1, ShardMapManager arg2, StoreShardMap arg3) -> CreateGetShardsGlobalOperationStringShardMapManagerIStoreShardMap
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(operationName, shardMapManager, shardMap);
    }
    if (this.___callBase) {
      return super.createGetShardsGlobalOperation(operationName, shardMapManager, shardMap);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetShardsGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createGetShardsLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName)
   */
  @Override
  public IStoreOperationLocal createGetShardsLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName) {
    Func3Param<ShardMapManager, ShardLocation, String, IStoreOperationLocal> func1 = (ShardMapManager arg1, ShardLocation arg2, String arg3) -> CreateGetShardsLocalOperationShardMapManagerShardLocationString
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, location, operationName);
    }
    if (this.___callBase) {
      return super.createGetShardsLocalOperation(shardMapManager, location, operationName);
    }
    return this.getInstanceBehavior().Result(this,
        "createGetShardsLocalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createLoadShardMapManagerGlobalOperation(ShardMapManager
   * shardMapManager, String operationName)
   */
  @Override
  public IStoreOperationGlobal createLoadShardMapManagerGlobalOperation(
      ShardMapManager shardMapManager, String operationName) {
    Func2Param<ShardMapManager, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2) -> CreateLoadShardMapManagerGlobalOperationShardMapManagerString
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName);
    }
    if (this.___callBase) {
      return super.createLoadShardMapManagerGlobalOperation(shardMapManager, operationName);
    }
    return this.getInstanceBehavior().Result(this,
        "createLoadShardMapManagerGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createLockOrUnLockMappingsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreMapping mapping, Guid
   * lockOwnerId, LockOwnerIdOpType lockOpType, ShardManagementErrorCategory errorCategory)
   */
  @Override
  public IStoreOperationGlobal createLockOrUnLockMappingsGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreShardMap shardMap,
      StoreMapping mapping, UUID lockOwnerId, LockOwnerIdOpType lockOpType,
      ShardManagementErrorCategory errorCategory) {
    Func7Param<ShardMapManager, String, StoreShardMap, StoreMapping, UUID, LockOwnerIdOpType, ShardManagementErrorCategory, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, StoreMapping arg4, UUID arg5, LockOwnerIdOpType arg6, ShardManagementErrorCategory arg7) -> CreateLockOrUnLockMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreMappingGuidLockOwnerIdOpTypeShardManagementErrorCategory
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    if (func1 != null) {
      return func1
          .invoke(shardMapManager, operationName, shardMap, mapping, lockOwnerId, lockOpType,
              errorCategory);
    }
    if (this.___callBase) {
      return super
          .createLockOrUnLockMappingsGlobalOperation(shardMapManager, operationName, shardMap,
              mapping, lockOwnerId, lockOpType, errorCategory);
    }
    return this.getInstanceBehavior().Result(this,
        "createLockOrUnLockMappingsGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createRemoveMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mapping, Guid lockOwnerId)
   */
  @Override
  public IStoreOperation createRemoveMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mapping,
      UUID lockOwnerId) {
    Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, UUID, IStoreOperation> func1 = (ShardMapManager arg1, StoreOperationCode arg2, StoreShardMap arg3, StoreMapping arg4, UUID arg5) -> CreateRemoveMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingGuid
        .invoke(arg1, arg2, arg3, arg4, arg5);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationCode, shardMap, mapping, lockOwnerId);
    }
    if (this.___callBase) {
      return super.createRemoveMappingOperation(shardMapManager, operationCode, shardMap, mapping,
          lockOwnerId);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveMappingOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createRemoveMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      JAXBElement root, UUID originalShardVersionRemoves) {
    Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> func1 = (StoreOperationCode arg1, ShardMapManager arg2, UUID arg3, StoreOperationState arg4, JAXBElement arg5, UUID arg6) -> CreateRemoveMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6);
    if (func1 != null) {
      return func1.invoke(operationCode, shardMapManager, operationId, undoStartState, root,
          originalShardVersionRemoves);
    }
    if (this.___callBase) {
      return super
          .createRemoveMappingOperation(operationCode, shardMapManager, operationId, undoStartState,
              root, originalShardVersionRemoves);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveMappingOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardMapGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap)
   */
  @Override
  public IStoreOperationGlobal createRemoveShardMapGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap) {
    Func3Param<ShardMapManager, String, StoreShardMap, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3) -> CreateRemoveShardMapGlobalOperationShardMapManagerStringIStoreShardMap
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, shardMap);
    }
    if (this.___callBase) {
      return super.createRemoveShardMapGlobalOperation(shardMapManager, operationName, shardMap);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveShardMapGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shard)
   */
  @Override
  public IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shard) {
    Func3Param<ShardMapManager, StoreShardMap, StoreShard, IStoreOperation> func1 = (ShardMapManager arg1, StoreShardMap arg2, StoreShard arg3) -> CreateRemoveShardOperationShardMapManagerIStoreShardMapIStoreShard
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, shardMap, shard);
    }
    if (this.___callBase) {
      return super.createRemoveShardOperation(shardMapManager, shardMap, shard);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveShardOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createRemoveShardOperation(ShardMapManager shardMapManager,
      UUID operationId, StoreOperationState undoStartState, JAXBElement root) {
    Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> func1 = (ShardMapManager arg1, UUID arg2, StoreOperationState arg3, JAXBElement arg4) -> CreateRemoveShardOperationShardMapManagerGuidStoreOperationStateXElement
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationId, undoStartState, root);
    }
    if (this.___callBase) {
      return super.createRemoveShardOperation(shardMapManager, operationId, undoStartState, root);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveShardOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createRemoveShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, String schemaInfoName)
   */
  @Override
  public IStoreOperationGlobal createRemoveShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, String schemaInfoName) {
    Func3Param<ShardMapManager, String, String, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, String arg3) -> CreateRemoveShardingSchemaInfoGlobalOperationShardMapManagerStringString
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, schemaInfoName);
    }
    if (this.___callBase) {
      return super.createRemoveShardingSchemaInfoGlobalOperation(shardMapManager, operationName,
          schemaInfoName);
    }
    return this.getInstanceBehavior().Result(this,
        "createRemoveShardingSchemaInfoGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreShardMap shardMap, StoreShard shard,
   * IEnumerable`1&lt;StoreMapping&gt; mappingsToRemove, IEnumerable`1&lt;StoreMapping&gt;
   * mappingsToAdd)
   */
  @Override
  public IStoreOperationGlobal createReplaceMappingsGlobalOperation(ShardMapManager shardMapManager,
      String operationName, StoreShardMap shardMap, StoreShard shard,
      List<StoreMapping> mappingsToRemove, List<StoreMapping> mappingsToAdd) {
    Func6Param<ShardMapManager, String, StoreShardMap, StoreShard, List<StoreMapping>, List<StoreMapping>, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreShardMap arg3, StoreShard arg4, List<StoreMapping> arg5, List<StoreMapping> arg6) -> CreateReplaceMappingsGlobalOperationShardMapManagerStringIStoreShardMapIStoreShardIEnumerableOfIStoreMappingIEnumerableOfIStoreMapping
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6);
    if (func1 != null) {
      return func1
          .invoke(shardMapManager, operationName, shardMap, shard, mappingsToRemove, mappingsToAdd);
    }
    if (this.___callBase) {
      return super
          .createReplaceMappingsGlobalOperation(shardMapManager, operationName, shardMap, shard,
              mappingsToRemove, mappingsToAdd);
    }
    return this.getInstanceBehavior().Result(this,
        "createReplaceMappingsGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, StoreShardMap shardMap,
   * StoreShard shard, IEnumerable`1&lt;ShardRange&gt; rangesToRemove,
   * IEnumerable`1&lt;StoreMapping&gt; mappingsToAdd)
   */
  @Override
  public IStoreOperationLocal createReplaceMappingsLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName, StoreShardMap shardMap, StoreShard shard,
      List<ShardRange> rangesToRemove, List<StoreMapping> mappingsToAdd) {
    Func7Param<ShardMapManager, ShardLocation, String, StoreShardMap, StoreShard, List<ShardRange>, List<StoreMapping>, IStoreOperationLocal> func1 = (ShardMapManager arg1, ShardLocation arg2, String arg3, StoreShardMap arg4, StoreShard arg5, List<ShardRange> arg6, List<StoreMapping> arg7) -> CreateReplaceMappingsLocalOperationShardMapManagerShardLocationStringIStoreShardMapIStoreShardIEnumerableOfShardRangeIEnumerableOfIStoreMapping
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    if (func1 != null) {
      return func1.invoke(shardMapManager, location, operationName, shardMap, shard, rangesToRemove,
          mappingsToAdd);
    }
    if (this.___callBase) {
      return super
          .createReplaceMappingsLocalOperation(shardMapManager, location, operationName, shardMap,
              shard, rangesToRemove, mappingsToAdd);
    }
    return this.getInstanceBehavior().Result(this,
        "createReplaceMappingsLocalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createReplaceMappingsOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap,
   * Pair`2&lt;StoreMapping,Guid&gt;[] mappingsSource, Pair`2&lt;StoreMapping,Guid&gt;[]
   * mappingsTarget)
   */
  @Override
  public IStoreOperation createReplaceMappingsOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap,
      List<Pair<StoreMapping, UUID>> mappingsSource,
      List<Pair<StoreMapping, UUID>> mappingsTarget) {
    Func5Param<ShardMapManager, StoreOperationCode, StoreShardMap, List<Pair<StoreMapping, UUID>>, List<Pair<StoreMapping, UUID>>, IStoreOperation> func1 = (ShardMapManager arg1, StoreOperationCode arg2, StoreShardMap arg3, List<Pair<StoreMapping, UUID>> arg4, List<Pair<StoreMapping, UUID>> arg5) -> CreateReplaceMappingsOperationShardMapManagerStoreOperationCodeIStoreShardMapTupleOfIStoreMappingGuidArrayTupleOfIStoreMappingGuidArray
        .invoke(arg1, arg2, arg3, arg4, arg5);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationCode, shardMap, mappingsSource, mappingsTarget);
    }
    if (this.___callBase) {
      return super
          .createReplaceMappingsOperation(shardMapManager, operationCode, shardMap, mappingsSource,
              mappingsTarget);
    }
    return this.getInstanceBehavior().Result(this,
        "createReplaceMappingsOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createReplaceMappingsOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      JAXBElement root, UUID originalShardVersionAdds) {
    Func6Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, IStoreOperation> func1 = (StoreOperationCode arg1, ShardMapManager arg2, UUID arg3, StoreOperationState arg4, JAXBElement arg5, UUID arg6) -> CreateReplaceMappingsOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuid
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6);
    if (func1 != null) {
      return func1.invoke(operationCode, shardMapManager, operationId, undoStartState, root,
          originalShardVersionAdds);
    }
    if (this.___callBase) {
      return super.createReplaceMappingsOperation(operationCode, shardMapManager, operationId,
          undoStartState, root, originalShardVersionAdds);
    }
    return this.getInstanceBehavior().Result(this,
        "createReplaceMappingsOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createUpdateMappingOperation(ShardMapManager
   * shardMapManager, StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping
   * mappingSource, StoreMapping mappingTarget, String patternForKill, Guid lockOwnerId)
   */
  @Override
  public IStoreOperation createUpdateMappingOperation(ShardMapManager shardMapManager,
      StoreOperationCode operationCode, StoreShardMap shardMap, StoreMapping mappingSource,
      StoreMapping mappingTarget, String patternForKill, UUID lockOwnerId) {
    Func7Param<ShardMapManager, StoreOperationCode, StoreShardMap, StoreMapping, StoreMapping, String, UUID, IStoreOperation> func1 = (ShardMapManager arg1, StoreOperationCode arg2, StoreShardMap arg3, StoreMapping arg4, StoreMapping arg5, String arg6, UUID arg7) -> CreateUpdateMappingOperationShardMapManagerStoreOperationCodeIStoreShardMapIStoreMappingIStoreMappingStringGuid
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationCode, shardMap, mappingSource, mappingTarget,
          patternForKill, lockOwnerId);
    }
    if (this.___callBase) {
      return super
          .createUpdateMappingOperation(shardMapManager, operationCode, shardMap, mappingSource,
              mappingTarget, patternForKill, lockOwnerId);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpdateMappingOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createUpdateMappingOperation(StoreOperationCode operationCode,
      ShardMapManager shardMapManager, UUID operationId, StoreOperationState undoStartState,
      JAXBElement root, UUID originalShardVersionRemoves, UUID originalShardVersionAdds) {
    Func7Param<StoreOperationCode, ShardMapManager, UUID, StoreOperationState, JAXBElement, UUID, UUID, IStoreOperation> func1 = (StoreOperationCode arg1, ShardMapManager arg2, UUID arg3, StoreOperationState arg4, JAXBElement arg5, UUID arg6, UUID arg7) -> CreateUpdateMappingOperationStoreOperationCodeShardMapManagerGuidStoreOperationStateXElementGuidGuid
        .invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    if (func1 != null) {
      return func1.invoke(operationCode, shardMapManager, operationId, undoStartState, root,
          originalShardVersionRemoves, originalShardVersionAdds);
    }
    if (this.___callBase) {
      return super
          .createUpdateMappingOperation(operationCode, shardMapManager, operationId, undoStartState,
              root, originalShardVersionRemoves, originalShardVersionAdds);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpdateMappingOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createUpdateShardOperation(ShardMapManager
   * shardMapManager, StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew)
   */
  @Override
  public IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager,
      StoreShardMap shardMap, StoreShard shardOld, StoreShard shardNew) {
    Func4Param<ShardMapManager, StoreShardMap, StoreShard, StoreShard, IStoreOperation> func1 = (ShardMapManager arg1, StoreShardMap arg2, StoreShard arg3, StoreShard arg4) -> CreateUpdateShardOperationShardMapManagerIStoreShardMapIStoreShardIStoreShard
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, shardMap, shardOld, shardNew);
    }
    if (this.___callBase) {
      return super.createUpdateShardOperation(shardMapManager, shardMap, shardOld, shardNew);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpdateShardOperation");
  }

  //TODO Check why @Override throws error
  public IStoreOperation createUpdateShardOperation(ShardMapManager shardMapManager,
      UUID operationId, StoreOperationState undoStartState, JAXBElement root) {
    Func4Param<ShardMapManager, UUID, StoreOperationState, JAXBElement, IStoreOperation> func1 = (ShardMapManager arg1, UUID arg2, StoreOperationState arg3, JAXBElement arg4) -> CreateUpdateShardOperationShardMapManagerGuidStoreOperationStateXElement
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationId, undoStartState, root);
    }
    if (this.___callBase) {
      return super.createUpdateShardOperation(shardMapManager, operationId, undoStartState, root);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpdateShardOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createUpdateShardingSchemaInfoGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, StoreSchemaInfo schemaInfo)
   */
  @Override
  public IStoreOperationGlobal createUpdateShardingSchemaInfoGlobalOperation(
      ShardMapManager shardMapManager, String operationName, StoreSchemaInfo schemaInfo) {
    Func3Param<ShardMapManager, String, StoreSchemaInfo, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, StoreSchemaInfo arg3) -> CreateUpdateShardingSchemaInfoGlobalOperationShardMapManagerStringIStoreSchemaInfo
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, schemaInfo);
    }
    if (this.___callBase) {
      return super.createUpdateShardingSchemaInfoGlobalOperation(shardMapManager, operationName,
          schemaInfo);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpdateShardingSchemaInfoGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createUpgradeStoreGlobalOperation(ShardMapManager
   * shardMapManager, String operationName, Version targetVersion)
   */
  @Override
  public IStoreOperationGlobal createUpgradeStoreGlobalOperation(ShardMapManager shardMapManager,
      String operationName, Version targetVersion) {
    Func3Param<ShardMapManager, String, Version, IStoreOperationGlobal> func1 = (ShardMapManager arg1, String arg2, Version arg3) -> CreateUpgradeStoreGlobalOperationShardMapManagerStringVersion
        .invoke(arg1, arg2, arg3);
    if (func1 != null) {
      return func1.invoke(shardMapManager, operationName, targetVersion);
    }
    if (this.___callBase) {
      return super.createUpgradeStoreGlobalOperation(shardMapManager, operationName, targetVersion);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpgradeStoreGlobalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.createUpgradeStoreLocalOperation(ShardMapManager
   * shardMapManager, ShardLocation location, String operationName, Version targetVersion)
   */
  @Override
  public IStoreOperationLocal createUpgradeStoreLocalOperation(ShardMapManager shardMapManager,
      ShardLocation location, String operationName, Version targetVersion) {
    Func4Param<ShardMapManager, ShardLocation, String, Version, IStoreOperationLocal> func1 = (ShardMapManager arg1, ShardLocation arg2, String arg3, Version arg4) -> CreateUpgradeStoreLocalOperationShardMapManagerShardLocationStringVersion
        .invoke(arg1, arg2, arg3, arg4);
    if (func1 != null) {
      return func1.invoke(shardMapManager, location, operationName, targetVersion);
    }
    if (this.___callBase) {
      return super.createUpgradeStoreLocalOperation(shardMapManager, location, operationName,
          targetVersion);
    }
    return this.getInstanceBehavior().Result(this,
        "createUpgradeStoreLocalOperation");
  }

  /**
   * Sets the stub of StoreOperationFactory.fromLogEntry(ShardMapManager shardMapManager,
   * StoreLogEntry so)
   */
  @Override
  public IStoreOperation fromLogEntry(ShardMapManager shardMapManager, StoreLogEntry so) {
    Func2Param<ShardMapManager, StoreLogEntry, IStoreOperation> func1 = (ShardMapManager arg1, StoreLogEntry arg2) -> FromLogEntryShardMapManagerIStoreLogEntry
        .invoke(arg1, arg2);
    if (func1 != null) {
      return func1.invoke(shardMapManager, so);
    }
    if (this.___callBase) {
      return super.fromLogEntry(shardMapManager, so);
    }
    return this.getInstanceBehavior().Result(this,
        "fromLogEntry");
  }

  /**
   * Initializes a new instance of type StubStoreOperationFactory
   */
  private void InitializeStub() {
  }
}