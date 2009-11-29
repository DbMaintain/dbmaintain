package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class TrivialExpression implements Expression {

    private final boolean outcome;

    public TrivialExpression(boolean outcome) {
        this.outcome = outcome;
    }

    public boolean evaluate(OperandResolver operandResolver) {
        return outcome;
    }
}
