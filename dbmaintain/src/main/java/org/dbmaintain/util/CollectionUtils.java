/*
 * Copyright 2006-2007,  Unitils.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbmaintain.util;

import static java.util.Arrays.asList;

import java.util.*;

/**
 * Class containing collection related utilities
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CollectionUtils {


    /**
     * Gets a list containing all elements from the given index to the given index.
     *
     * @param list      The original list
     * @param fromIndex The from index
     * @param toIndex   The to index
     * @return The sub-list, not null
     */
    public static <T> List<T> subList(List<T> list, int fromIndex, int toIndex) {
        List<T> result = new ArrayList<T>();
        if (list == null) {
            return result;
        }
        for (int i = fromIndex; i < toIndex; i++) {
            result.add(list.get(i));
        }
        return result;
    }


    /**
     * Converts the given array of elements to a set.
     *
     * @param elements The elements
     * @return The elements as a set, empty if elements was null
     */
    public static <T> Set<T> asSet(T... elements) {
        Set<T> result = new HashSet<T>();
        if (elements == null) {
            return result;
        }
        result.addAll(asList(elements));
        return result;
    }


    /**
     * Converts the given array of elements to a sortedset.
     *
     * @param elements The elements
     * @return The elements as a set, empty if elements was null
     */
    public static <T> SortedSet<T> asSortedSet(T... elements) {
        SortedSet<T> result = new TreeSet<T>();
        if (elements == null) {
            return result;
        }
        result.addAll(asList(elements));
        return result;
    }
    
    
    @SuppressWarnings("unchecked")
	public static <T> Set<T> castGenericType(Set<?> set) {
    	Set tmp = set;
    	return (Set<T>) tmp;
    }


    public static <T> SortedSet<T> unionSortedSet(Set<T>... sets) {
        SortedSet<T> unionSet = new TreeSet<T>();
        for (Set<T> set : sets) {
            unionSet.addAll(set);
        }
        return unionSet;
    }
}
