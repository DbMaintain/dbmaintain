package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class TrivialExpression implements Expression {

    public boolean evaluate(OperandResolver operandResolver) {
        return true;
    }
}
