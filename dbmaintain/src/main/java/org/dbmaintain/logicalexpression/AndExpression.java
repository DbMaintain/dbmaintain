package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class AndExpression implements Expression {

    private final Expression left;
    private final Expression right;

    public AndExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public boolean evaluate(OperandResolver operandResolver) {
        return left.evaluate(operandResolver) && right.evaluate(operandResolver);
    }
}
