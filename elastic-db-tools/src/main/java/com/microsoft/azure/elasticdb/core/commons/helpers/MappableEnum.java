package com.microsoft.azure.elasticdb.core.commons.helpers;

/**
 * Allows {@link EnumHelpers#createMap(Class)} to query the internal value
 * @author sulrich
 *
 */
public interface MappableEnum {

	public int getValue();
	
}
