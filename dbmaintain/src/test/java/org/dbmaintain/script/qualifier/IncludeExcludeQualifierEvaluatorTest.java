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

import org.dbmaintain.script.qualifier.impl.IncludeExcludeQualifierEvaluator;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class IncludeExcludeQualifierEvaluatorTest {

    @Test
    public void includedQualifiers() {
        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(qualifiers("Q1"), qualifiers());
        assertTrue(evaluator.evaluate(qualifiers("Q1")));
        assertTrue(evaluator.evaluate(qualifiers("Q1", "Q3")));
        assertFalse(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q3")));
    }

    @Test
    public void excludedQualifiers() {
        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(qualifiers(), qualifiers("Q1"));
        assertTrue(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q1")));
    }

    @Test
    public void includedAndExcludedQualifiers() {
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
