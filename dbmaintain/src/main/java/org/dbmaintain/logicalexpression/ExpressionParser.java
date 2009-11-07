package org.dbmaintain.logicalexpression;

import java.util.List;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ExpressionParser {

    private final ExpressionTokenizer tokenizer = new ExpressionTokenizer();
    private final BraceAnalyser braceAnalyser = new BraceAnalyser();
    private final AtomicOperandValidator atomicOperandValidator;

    public ExpressionParser(AtomicOperandValidator atomicOperandValidator) {
        this.atomicOperandValidator = atomicOperandValidator;
    }

    public Expression parse(String expressionStr) {
        return parse(tokenizer.tokenize(expressionStr));
    }

    private Expression parse(List<String> expressionTokens) {
        return evaluate(new TrivialExpression(), expressionTokens);
    }

    private Expression evaluate(Expression currentExpression, List<String> expressionTokens) {
        if (expressionTokens.isEmpty())
            return currentExpression;
        else {
            String head = expressionTokens.get(0);
            List<String> tail = getTail(expressionTokens);
            if ("&&".equals(head)) {
                return new AndExpression(currentExpression, parse(tail));
            } else if ("||".equals(head)) {
                return new OrExpression(currentExpression, parse(tail));
            } else if ("!".equals(head)) {
                return new NotExpression(parse(tail));
            } else if ("(".equals(head)) {
                BraceAnalyser.SplitExpression splitExpression = braceAnalyser.splitOffBracedSubExpression(tail);
                return evaluate(parse(splitExpression.getBracedSubExpression()), splitExpression.getRemainder());
            } else if (")".equals(head)) {
                throw new IllegalArgumentException("Expression contains closing brace without an accompanying opening brace");
            } else {
                // If none of the previous conditions hold, we have an atomic operand
                atomicOperandValidator.validateOperandName(head);
                return evaluate(new AtomicExpression(head), tail);
            }
        }
    }

    private List<String> getTail(List<String> expressionTokens) {
        return expressionTokens.subList(1, expressionTokens.size());
    }
}
