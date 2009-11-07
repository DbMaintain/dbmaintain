package org.dbmaintain.logicalexpression;

import java.util.List;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class BraceAnalyser {

    public SplitExpression splitOffBracedSubExpression(List<String> expressionTokens) {
        int tokenCounter = 0;
        int openBracesCount = 0;
        for (String expressionToken : expressionTokens) {
            if (")".equals(expressionToken) && openBracesCount == 0) {
                return new SplitExpression(expressionTokens.subList(0, tokenCounter),
                        expressionTokens.subList(tokenCounter + 1, expressionTokens.size()));
            } else if (")".equals(expressionToken))
                openBracesCount -= 1;
            else if ("(".equals(expressionToken))
                openBracesCount += 1;
            tokenCounter += 1;
        }
        throw new IllegalArgumentException("Expression contains an opening brace without an accompanying closing brace");
    }

    public static class SplitExpression {

        private final List<String> bracedSubExpression;
        private final List<String> remainder;

        public SplitExpression(List<String> bracedSubExpression, List<String> remainder) {
            this.bracedSubExpression = bracedSubExpression;
            this.remainder = remainder;
        }

        public List<String> getBracedSubExpression() {
            return bracedSubExpression;
        }

        public List<String> getRemainder() {
            return remainder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SplitExpression that = (SplitExpression) o;

            if (bracedSubExpression != null ? !bracedSubExpression.equals(that.bracedSubExpression) : that.bracedSubExpression != null)
                return false;
            if (remainder != null ? !remainder.equals(that.remainder) : that.remainder != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = bracedSubExpression != null ? bracedSubExpression.hashCode() : 0;
            result = 31 * result + (remainder != null ? remainder.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "SplitExpression{" +
                    "bracedSubExpression=" + bracedSubExpression +
                    ", remainder=" + remainder +
                    '}';
        }
    }

}
