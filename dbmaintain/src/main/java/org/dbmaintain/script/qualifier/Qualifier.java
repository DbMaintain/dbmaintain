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
package org.dbmaintain.script.qualifier;

import java.util.Objects;

/**
 * Represents a script qualifier, that characterizes a database script in some way, and that
 * can be used to exclude scripts from execution or to indicate 'patch' scripts.
 *
 * @author Filip Neven
 */
public class Qualifier {

    private final String qualifierName;

    public Qualifier(String qualifierName) {
        this.qualifierName = qualifierName.toLowerCase();
    }

    public String getQualifierName() {
        return qualifierName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Qualifier qualifier = (Qualifier) o;

        if (!Objects.equals(qualifierName, qualifier.qualifierName))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return qualifierName != null ? qualifierName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Qualifier{" + "qualifierName='" + qualifierName + '\'' + '}';
    }
}
