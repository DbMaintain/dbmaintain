package org.dbmaintain.script;

import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class IncludeExcludeQualifierEvaluator implements QualifierEvaluator {

    private final Set<Qualifier> includedQualifiers;
    private final Set<Qualifier> excludedQualifiers;


    public IncludeExcludeQualifierEvaluator(Set<Qualifier> includedQualifiers, Set<Qualifier> excludedQualifiers) {
        this.includedQualifiers = includedQualifiers;
        this.excludedQualifiers = excludedQualifiers;
    }

    public boolean evaluate(Set<Qualifier> qualifiers) {
        return (includedQualifiers.isEmpty() || containsIncludedQualifier(qualifiers))
            && !containsExcludedQualifier(qualifiers);
    }

    private boolean containsExcludedQualifier(Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers) {
            if (excludedQualifiers.contains(qualifier)) return true;
        }
        return false;
    }

    protected boolean containsIncludedQualifier(Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers) {
            if (includedQualifiers.contains(qualifier)) return true;
        }
        return false;
    }
}
