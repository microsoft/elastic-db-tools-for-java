/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 *
 */

package com.microsoft.azure.elasticdb.shard.base;

import java.time.ZonedDateTime;
import java.util.Objects;

import com.google.common.base.Preconditions;

public class ShardKey {
	private final ShardKeyType type;
	private final Object value;
	private final int _hashCode;
	
	public ShardKey(Integer value) {
		this(ShardKeyType.Int, value);
	}
	
	public ShardKey(Long value) {
		this(ShardKeyType.Long, value);
	}
	
	public ShardKey(String guid) {
		this(ShardKeyType.Guid, guid);
	}
	
	public ShardKey(ZonedDateTime dateTime) {
		this(ShardKeyType.DateTime, dateTime);
	}
	
	public ShardKey(Object value) {
		this(ShardKeyType.None, value);
	}
	
	public ShardKey(ShardKeyType type, Object value) {
		this.type = Preconditions.checkNotNull(type);
		this.value = Preconditions.checkNotNull(value);
		_hashCode = Objects.hash(type, value);
	}
	
	public ShardKeyType getKeyType() {
		return type;
	}
	
	public Integer getInteger() {
		Preconditions.checkArgument(type == ShardKeyType.Int);
		return (Integer)value;
	}
	
	@Override
    public boolean equals(Object other) {
    	if(other == null || !(other instanceof ShardKey)) {
    		return false;
    	}
    	ShardKey otherObj = (ShardKey) other;
    	return Objects.equals(type, otherObj.type)
			&& Objects.equals(value, otherObj.value);
    }
	
	@Override
	public int hashCode() {
		return _hashCode;
	}
}
