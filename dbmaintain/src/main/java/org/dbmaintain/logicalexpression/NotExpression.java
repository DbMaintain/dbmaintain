package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class NotExpression implements Expression {

    private final Expression expression;

    public NotExpression(Expression expression) {
        this.expression = expression;
    }

    public boolean evaluate(OperandResolver operandResolver) {
        return !expression.evaluate(operandResolver);
    }
}
