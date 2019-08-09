package com.microsoft.azure.elasticdb.core.commons.helpers;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EnumHelpers {

	/** 
	 * Creates a map out of an {@link MappableEnum}, mapping the {@link MappableEnum#getValue()} value to the enum constant
	 * 
	 * @param type class of the enum to map
	 * @return an unmodifiable map containing all enum values mapped by their values.
	 */
	public static <E extends Enum<E> & MappableEnum> Map<Integer, E> createMap(Class<E> type) {
		
		Map<Integer, E> tmp = EnumSet.allOf(type)//
				.stream() //
				.collect(Collectors.toMap(e -> e.getValue(), Function.identity()));
    	return Collections.unmodifiableMap(tmp);
	}
	
}
