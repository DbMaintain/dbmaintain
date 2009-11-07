package org.dbmaintain.logicalexpression;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;

import static java.util.Arrays.asList;
import java.util.List;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ExpressionTokenizerTest {

    private ExpressionTokenizer expressionTokenizer = new ExpressionTokenizer();

    @Test
    public void testTokenize() {
        assertEquals(asList("a", "&&", "b"), tokenize("a && b"));
        assertEquals(asList("!", "a"), tokenize("!a"));
        assertEquals(asList("(", "(", "a", ")", ")"), tokenize("((a))"));
    }

    private List tokenize(String expression) {
        return expressionTokenizer.tokenize(expression);
    }
}
