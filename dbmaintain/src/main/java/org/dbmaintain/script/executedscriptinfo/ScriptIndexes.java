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
package org.dbmaintain.script.executedscriptinfo;

import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.split;

/**
 * Class representing the indexes of a script.
 * <p>
 * Some examples:
 * <p>
 * 01_folder/01_subfolder/1_script  ==&gt; 1,1,1<br>
 * 01_folder/02_subfolder/1_script  ==&gt; 1,2,1<br>
 * 01_folder/02_subfolder/script    ==&gt; 1,2,null<br>
 * folder/subfolder/2_script        ==&gt; null,null,2<br>
 * script                           ==&gt; null<br>
 * <p>
 * The last index defines whether the script is incremental or repeatable: if the last index is null, the
 * script is repeatable; if not, it is incremental.
 *
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ScriptIndexes implements Comparable<ScriptIndexes> {

    /* The version indexes, empty if not defined */
    private List<Long> indexes = new ArrayList<Long>();


    /**
     * Creates a new version.
     *
     * @param indexes The script indexes, not null
     */
    public ScriptIndexes(List<Long> indexes) {
        this.indexes = indexes;
        assertValidIndexes();
    }


    /**
     * Creates a new version.
     *
     * @param indexString The indexes as a string
     */
    public ScriptIndexes(String indexString) {
        this.indexes = extractIndexes(indexString);
        assertValidIndexes();
    }


    /**
     * An empty list if no version is defined.
     *
     * @return The script index, not null
     */
    public List<Long> getIndexes() {
        return indexes;
    }


    public boolean isIncrementalScript() {
        return !indexes.isEmpty() && indexes.get(indexes.size() - 1) != null;
    }


    public boolean isRepeatableScript() {
        return !isIncrementalScript();
    }


    protected void assertValidIndexes() {
        if (isRepeatableScript()) {
            for (Long index : indexes) {
                if (index != null) {
                    throw new DbMaintainException("Repeatable scripts cannot be located inside an indexed folder.");
                }
            }
        }
    }


    /**
     * Gets a string representation of the indexes as followes:
     * 1, null, 2, null =&gt; 1.x.2.x
     *
     * @return The string, not null
     */
    public String getIndexesString() {
        StringBuilder result = new StringBuilder();

        boolean first = true;
        for (Long index : indexes) {
            if (first) {
                first = false;
            } else {
                result.append('.');
            }
            if (index == null) {
                result.append('x');
            } else {
                result.append(index);
            }
        }
        return result.toString();
    }


    /**
     * Extracts the indexes out of the given string as followes:
     * 1.x.2.x =&gt; 1, null, 2, null
     *
     * @param indexString The string
     * @return The list of longs or nulls in case of 'x'
     */
    protected List<Long> extractIndexes(String indexString) {
        List<Long> result = new ArrayList<Long>();
        if (isEmpty(indexString)) {
            return result;
        }

        String[] parts = split(indexString, '.');
        for (String part : parts) {
            if ("x".equalsIgnoreCase(part)) {
                result.add(null);
            } else {
                result.add(new Long(part));
            }
        }
        return result;
    }


    /**
     * @return The string representation of the version.
     */
    @Override
    public String toString() {
        return "indexes: " + getIndexesString();
    }


    /**
     * Compares the given version to this version using the index values.
     * <p>
     * If both scripts have an index, the index is used.
     * If one of the scripts has an index, it is considerer lower than the script that does not have an index.
     *
     * @param otherVersion The other version, not null
     * @return -1 when this version is smaller, 0 if equal, 1 when larger
     */
    public int compareTo(ScriptIndexes otherVersion) {
        List<Long> otherIndexes = otherVersion.getIndexes();
        if (indexes.isEmpty()) {
            if (otherIndexes.isEmpty()) {
                return 0;
            }
            return -1;
        } else if (otherIndexes.isEmpty()) {
            return 1;
        }
        Iterator<Long> thisIterator = indexes.iterator();
        Iterator<Long> otherIterator = otherIndexes.iterator();

        while (thisIterator.hasNext() && otherIterator.hasNext()) {
            Long thisIndex = thisIterator.next();
            Long otherIndex = otherIterator.next();

            if (thisIndex != null && otherIndex != null) {
                if (thisIndex < otherIndex) {
                    return -1;
                }
                if (thisIndex > otherIndex) {
                    return 1;
                }
            } else if (thisIndex != null) {
                return -1;
            } else if (otherIndex != null) {
                return 1;
            }
        }
        if (!thisIterator.hasNext() && !otherIterator.hasNext()) {
            return 0;
        }
        if (thisIterator.hasNext()) {
            return 1;
        } else {
            return -1;
        }
    }


    /**
     * @return A computed hashcode value dependent on the indexes
     */
    @Override
    public int hashCode() {
        return 31 + ((indexes == null) ? 0 : indexes.hashCode());
    }


    /**
     * @param object The object to compare with
     * @return True if the given object has the same indexes
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (getClass() != object.getClass()) {
            return false;
        }
        ScriptIndexes other = (ScriptIndexes) object;
        if (indexes == null) {
            if (other.indexes != null) {
                return false;
            }
        } else if (!indexes.equals(other.indexes)) {
            return false;
        }
        return true;
    }


}
