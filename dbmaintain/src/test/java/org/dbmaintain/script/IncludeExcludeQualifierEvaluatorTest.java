package org.dbmaintain.script;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class IncludeExcludeQualifierEvaluatorTest {

    @Test public void includedQualifiers() {
        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(qualifiers("Q1"), qualifiers());
        assertTrue(evaluator.evaluate(qualifiers("Q1")));
        assertTrue(evaluator.evaluate(qualifiers("Q1", "Q3")));
        assertFalse(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q3")));
    }

    @Test public void excludedQualifiers() {
        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(qualifiers(), qualifiers("Q1"));
        assertTrue(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q1")));
    }

    @Test public void includedAndExcludedQualifiers() {
        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(qualifiers("Q1"), qualifiers("Q2"));
        assertFalse(evaluator.evaluate(qualifiers("Q1", "Q2")));
    }

    private Set<Qualifier> qualifiers(String... qualifiersStr) {
        Set<Qualifier> result = new HashSet<Qualifier>();
        for (String qualifierStr : qualifiersStr) {
            result.add(new Qualifier(qualifierStr));
        }
        return result;
    }
}
