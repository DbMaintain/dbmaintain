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
package org.dbmaintain.script.qualifier.impl;

import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.script.qualifier.QualifierEvaluator;

import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class IncludeExcludeQualifierEvaluator implements QualifierEvaluator {

    public static final Qualifier UNQUALIFIED = new Qualifier("<unqualified>");

    private final Set<Qualifier> includedQualifiers;
    private final Set<Qualifier> excludedQualifiers;


    public IncludeExcludeQualifierEvaluator(Set<Qualifier> registeredQualifiers, Set<Qualifier> includedQualifiers, Set<Qualifier> excludedQualifiers) {
        this.includedQualifiers = includedQualifiers;
        this.excludedQualifiers = excludedQualifiers;
        ensureQualifiersRegistered(registeredQualifiers, includedQualifiers);
        ensureQualifiersRegistered(registeredQualifiers, excludedQualifiers);
    }


    public boolean evaluate(Set<Qualifier> qualifiers) {
        return (includedQualifiers.isEmpty() || containsIncludedQualifier(qualifiers))
                && !containsExcludedQualifier(qualifiers);
    }


    protected void ensureQualifiersRegistered(Set<Qualifier> registeredQualifiers, Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers) {
            if (!UNQUALIFIED.equals(qualifier) && !registeredQualifiers.contains(qualifier)) {
                throw new IllegalArgumentException(qualifier + " is not registered");
            }
        }
    }

    protected boolean containsExcludedQualifier(Set<Qualifier> qualifiers) {
        if (qualifiers.isEmpty()) {
            return excludedQualifiers.contains(UNQUALIFIED);
        }
        for (Qualifier qualifier : qualifiers) {
            if (excludedQualifiers.contains(qualifier)) {
                return true;
            }
        }
        return false;
    }

    protected boolean containsIncludedQualifier(Set<Qualifier> qualifiers) {
        if (qualifiers.isEmpty()) {
            return includedQualifiers.contains(UNQUALIFIED);
        }
        for (Qualifier qualifier : qualifiers) {
            if (includedQualifiers.contains(qualifier)) {
                return true;
            }
        }
        return false;
    }

}
