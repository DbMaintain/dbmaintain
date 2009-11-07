package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class AtomicExpression implements Expression {
    
    private final String operandName;

    public AtomicExpression(String operandName) {
        this.operandName = operandName;
    }

    public boolean evaluate(OperandResolver operandResolver) {
        return operandResolver.resolveOperand(operandName);
    }
}
