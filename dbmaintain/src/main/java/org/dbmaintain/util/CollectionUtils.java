/*
 * Copyright DbMaintain.org
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

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Arrays.asList;

/**
 * Class containing collection related utilities
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CollectionUtils {

    /**
     * Converts the given array of elements to a sortedset.
     *
     * @param elements The elements
     * @return The elements as a set, empty if elements was null
     */
    public static <T> SortedSet<T> asSortedSet(T... elements) {
        SortedSet<T> result = new TreeSet<>();
        if (elements == null) {
            return result;
        }
        result.addAll(asList(elements));
        return result;
    }


    public static <T> SortedSet<T> unionSortedSet(Set<T>... sets) {
        SortedSet<T> unionSet = new TreeSet<>();
        for (Set<T> set : sets) {
            unionSet.addAll(set);
        }
        return unionSet;
    }
}
