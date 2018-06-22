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
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.dbmaintain.util.TestUtils.qualifiers;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class IncludeExcludeQualifierEvaluatorTest {

    @Test
    public void includedQualifiers() {
        Set<Qualifier> registeredQualifiers = qualifiers("Q1");
        Set<Qualifier> includedQualifiers = qualifiers("Q1");
        Set<Qualifier> excludedQualifiers = qualifiers();

        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
        assertTrue(evaluator.evaluate(qualifiers("Q1")));
        assertTrue(evaluator.evaluate(qualifiers("Q1", "Q3")));
        assertFalse(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q3")));
    }

    @Test
    public void includedQualifiers_unqualified() {
        Set<Qualifier> registeredQualifiers = qualifiers("Q1");
        Set<Qualifier> includedQualifiers = qualifiers("Q1", "<unqualified>");
        Set<Qualifier> excludedQualifiers = qualifiers();

        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
        assertTrue(evaluator.evaluate(qualifiers()));
    }


    @Test
    public void excludedQualifiers() {
        Set<Qualifier> registeredQualifiers = qualifiers("Q1");
        Set<Qualifier> includedQualifiers = qualifiers();
        Set<Qualifier> excludedQualifiers = qualifiers("Q1");

        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
        assertTrue(evaluator.evaluate(qualifiers()));
        assertTrue(evaluator.evaluate(qualifiers("Q3")));
        assertFalse(evaluator.evaluate(qualifiers("Q1")));
        assertFalse(evaluator.evaluate(qualifiers("Q1", "Q3")));
    }

    @Test
    public void excludedQualifiers_unqualified() {
        Set<Qualifier> registeredQualifiers = qualifiers("Q1");
        Set<Qualifier> includedQualifiers = qualifiers();
        Set<Qualifier> excludedQualifiers = qualifiers("Q1", "<unqualified>");

        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
        assertFalse(evaluator.evaluate(qualifiers()));
    }


    @Test
    public void includedAndExcludedQualifiers() {
        Set<Qualifier> registeredQualifiers = qualifiers("Q1", "Q2");
        Set<Qualifier> includedQualifiers = qualifiers("Q1");
        Set<Qualifier> excludedQualifiers = qualifiers("Q2", "<unqualified>");

        IncludeExcludeQualifierEvaluator evaluator = new IncludeExcludeQualifierEvaluator(registeredQualifiers, includedQualifiers, excludedQualifiers);
        assertTrue(evaluator.evaluate(qualifiers("Q1")));
        assertFalse(evaluator.evaluate(qualifiers()));
        assertFalse(evaluator.evaluate(qualifiers("Q2")));
        assertFalse(evaluator.evaluate(qualifiers("Q1", "Q2")));
    }
}
