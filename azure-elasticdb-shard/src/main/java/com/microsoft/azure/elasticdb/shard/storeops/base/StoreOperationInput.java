package com.microsoft.azure.elasticdb.shard.storeops.base;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.shard.base.ShardKey;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import com.microsoft.azure.elasticdb.shard.base.ShardRange;
import com.microsoft.azure.elasticdb.shard.store.StoreMapping;
import com.microsoft.azure.elasticdb.shard.store.StoreSchemaInfo;
import com.microsoft.azure.elasticdb.shard.store.StoreShard;
import com.microsoft.azure.elasticdb.shard.store.StoreShardMap;
import com.microsoft.azure.elasticdb.shard.store.Version;
import com.microsoft.azure.elasticdb.shard.storeops.base.StoreOperationRequestBuilder.Lock;
import com.microsoft.azure.elasticdb.shard.utils.GlobalConstants;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class StoreOperationInput {

  public static final Version S_GLOBAL
      = new Version(GlobalConstants.GsmVersionClient.getMajor(),
      GlobalConstants.GsmVersionClient.getMinor());

  public static final Version S_LOCAL
      = new Version(GlobalConstants.LsmVersionClient.getMajor(),
      GlobalConstants.LsmVersionClient.getMinor());

  @XmlElement(name = "GsmVersion")
  private Version gsmVersion;

  @XmlElement(name = "LsmVersion")
  private Version lsmVersion;

  @XmlAttribute(name = "OperationId")
  private UUID operationId;

  @XmlAttribute(name = "UndoStartState")
  private Integer undoStartState;

  @XmlElement(name = "ShardMap")
  private StoreShardMap shardMap;

  @XmlElement(name = "Location")
  private ShardLocation location;

  @XmlAttribute(name = "OperationCode")
  private StoreOperationCode operationCode;

  @XmlAttribute(name = "Undo")
  private Integer undo;

  @XmlElement(name = "Shard")
  private StoreShard shard;

  @XmlElement(name = "Range")
  private ShardRange range;

  @XmlElement(name = "Key")
  private ShardKey key;

  @XmlElement(name = "Mapping")
  private StoreMapping mapping;

  @XmlElement(name = "LockOwnerId")
  private UUID lockOwnerId;

  @XmlElement(name = "PatternForKill")
  private String patternForKill;

  @XmlElement(name = "SchemaInfo")
  private StoreSchemaInfo schemaInfo;

  @XmlElement(name = "ShardMapId")
  private UUID shardMapId;

  @XmlElement(name = "ShardId")
  private UUID shardId;

  @XmlElement(name = "ShardVersion")
  private UUID shardVersion;

  @XmlElement(name = "MappingId")
  private UUID mappingId;

  @XmlAttribute(name = "StepsCount")
  private Integer stepsCount;

  @XmlElement(name = "Steps")
  private StoreOperationRequestBuilder.Steps steps;

  @XmlAttribute(name = "Kind")
  private StoreOperationStepKind kind;

  @XmlAttribute(name = "Id")
  private Integer stepId;

  @XmlElement(name = "Removes")
  private StoreOperationInput removes;

  @XmlElement(name = "Adds")
  private StoreOperationInput adds;

  @XmlAttribute(name = "Validate")
  private Integer validate;

  @XmlElement(name = "Update")
  private StoreOperationInput update;

  @XmlElement(name = "Lock")
  private Lock lock;

  @XmlAttribute(name = "RemoveStepsCount")
  private int removeStepsCount;

  @XmlAttribute(name = "AddStepsCount")
  private int addStepsCount;

  @XmlElement(name = "RemoveSteps")
  private StoreOperationInput removeSteps;

  @XmlElement(name = "AddSteps")
  private StoreOperationInput addSteps;

  @XmlElement(name = "Pattern")
  private String pattern;

  private StoreOperationInput() {
  }

  public static class Builder {

    private StoreOperationInput input = new StoreOperationInput();

    public Builder() {
    }

    public Builder withGsmVersion() {
      input.gsmVersion = S_GLOBAL;
      return this;
    }

    public Builder withLsmVersion() {
      input.lsmVersion = S_LOCAL;
      return this;
    }

    public Builder withOperationId(UUID operationId) {
      input.operationId = operationId;
      return this;
    }

    public Builder withUndoStartState(StoreOperationState undoStartState) {
      input.undoStartState = undoStartState.getValue();
      return this;
    }

    public Builder withShardMap(StoreShardMap shardMap) {
      input.shardMap = shardMap;
      return this;
    }

    public Builder withLocation(ShardLocation location) {
      input.location = location;
      return this;
    }

    public Builder withOperationCode(StoreOperationCode operationCode) {
      input.operationCode = operationCode;
      return this;
    }

    public Builder withUndo(boolean undo) {
      input.undo = undo ? 1 : 0;
      return this;
    }

    public Builder withShard(StoreShard shard) {
      input.shard = shard;
      return this;
    }

    public Builder withShardRange(ShardRange range) {
      input.range = range;
      return this;
    }

    public Builder withShardKey(ShardKey key) {
      input.key = key;
      return this;
    }

    public Builder withMapping(StoreMapping mapping) {
      input.mapping = mapping;
      return this;
    }

    public Builder withLockOwnerId(UUID lockOwnerId) {
      input.lockOwnerId = lockOwnerId;
      return this;
    }

    public Builder withPatternForKill(String patternForKill) {
      input.patternForKill = patternForKill;
      return this;
    }

    public Builder withSchemaInfo(StoreSchemaInfo schemaInfo) {
      input.schemaInfo = schemaInfo;
      return this;
    }

    public Builder withShardMapId(UUID shardMapId) {
      input.shardMapId = shardMapId;
      return this;
    }

    public Builder withShardId(UUID shardId) {
      input.shardId = shardId;
      return this;
    }

    public Builder withShardVersion(UUID shardVersion) {
      input.shardVersion = shardVersion;
      return this;
    }

    public Builder withMappingId(UUID mappingId) {
      input.mappingId = mappingId;
      return this;
    }

    public Builder withStepsCount(int stepsCount) {
      input.stepsCount = stepsCount;
      return this;
    }

    public Builder withStoreOperationCode(StoreOperationCode operationCode) {
      input.operationCode = operationCode;
      return this;
    }

    public Builder withStoreMapping(StoreMapping mapping) {
      input.mapping = mapping;
      return this;
    }

    public Builder withSteps(List<StoreOperationInput> steps) {
      this.input.steps = new StoreOperationRequestBuilder.Steps(steps);
      return this;
    }

    public Builder withStoreOperationStepKind(StoreOperationStepKind kind) {
      input.kind = kind;
      return this;
    }

    public Builder withStepId(int id) {
      input.stepId = id;
      return this;
    }

    public Builder withRemoves(StoreOperationInput innerInput) {
      input.removes = innerInput;
      return this;
    }

    public Builder withAdds(StoreOperationInput innerInput) {
      input.adds = innerInput;
      return this;
    }

    public Builder withValidation(boolean validate) {
      input.validate = validate ? 1 : 0;
      return this;
    }

    public Builder withUpdate(StoreOperationInput innerInput) {
      input.update = innerInput;
      return this;
    }

    public Builder withLock(Lock lock) {
      input.lock = lock;
      return this;
    }

    public Builder withRemoveStepsCount(int count) {
      input.removeStepsCount = count;
      return this;
    }

    public Builder withAddStepsCount(int count) {
      input.addStepsCount = count;
      return this;
    }

    public Builder withRemoveSteps(StoreOperationInput input) {
      input.removeSteps = input;
      return this;
    }

    public Builder withAddSteps(StoreOperationInput input) {
      input.addSteps = input;
      return this;
    }

    public Builder withPattern(String pattern) {
      input.pattern = pattern;
      return this;
    }

    public StoreOperationInput build() {
      return input;
    }
  }
}


