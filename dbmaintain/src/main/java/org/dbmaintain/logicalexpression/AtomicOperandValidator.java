package org.dbmaintain.logicalexpression;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public interface AtomicOperandValidator {

    /**
     * Interface used by users of the expression evaluator to verify that the names of atomic operands in the expression
     * are valid. If the operand is not valid, an {@link IllegalArgumentException} must be thrown with a proper error
     * message.
     * @param operandName the name of the atomic operand, not null
     * @throws IllegalArgumentException if the given operandName is not valid
     */
    void validateOperandName(String operandName);
}
