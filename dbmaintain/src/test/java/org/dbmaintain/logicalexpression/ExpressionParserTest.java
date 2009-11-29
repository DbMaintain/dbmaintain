package org.dbmaintain.logicalexpression;

import org.junit.Test;
import org.junit.Ignore;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ExpressionParserTest {

    private OperandResolver operandResolver = new OperandResolver() {
        public boolean resolveOperand(String operandName) {
            return "T".equals(operandName);
        }
    };
    private AtomicOperandValidator atomicOperandValidator = new AtomicOperandValidator() {
        public void validateOperandName(String operandName) {
            if (!"T".equals(operandName) && !"F".equals(operandName)) {
                throw new IllegalArgumentException("Invalid operand name " + operandName);
            }
        }
    };
    private ExpressionParser parser = new ExpressionParser(atomicOperandValidator);

    @Test
    public void trivialExpressions() {
        assertTrue(evaluate("T"));
        assertFalse(evaluate("F"));
    }

    @Test
    public void andExpression() {
        assertTrue(evaluate("T && T"));
        assertFalse(evaluate("T && F"));
    }

    @Test
    public void orExpression() {
        assertTrue(evaluate("T || F"));
        assertFalse(evaluate("F || F"));
    }

    @Test
    public void notExpression() {
        assertTrue(evaluate("!F"));
    }

    @Test @Ignore
    // TODO an expression with multiple parts not organized with brackets is evaluated right-to-left: the opposite of
    // TODO what users reasonably expect. This test expects evaluation left-to-right.
    // TODO Solve this either by changing the evaluation order or, even better, by making sure && takes preference over ||
    public void combinedExpression() {
        assertFalse(evaluate("T || T && F"));
    }

    @Test
    public void expressionWithBrackets() {
        assertTrue(evaluate("T && (T || F)"));
        assertTrue(evaluate("(T || F) && T"));
        assertTrue(evaluate("!(F && T)"));
        assertTrue(evaluate("!F && T"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void expressionsWithErrors_closingBraceWithoutOpeningBrace() {
        evaluate(")");
    }


    @Test(expected = IllegalArgumentException.class)
    public void expressionsWithErrors_openingBraceWithoutClosingBrace() {
        evaluate(")");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidOperandName() {
        evaluate("X");
    }

    private boolean evaluate(String expression) {
        return parser.parse(expression).evaluate(operandResolver);
    }
}
