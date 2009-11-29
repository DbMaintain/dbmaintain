package org.dbmaintain.script;

import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface QualifierEvaluator {
    boolean evaluate(Set<Qualifier> qualifiers);
}
