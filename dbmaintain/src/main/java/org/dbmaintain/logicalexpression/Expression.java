package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface Expression {

    boolean evaluate(OperandResolver operandResolver);
}
