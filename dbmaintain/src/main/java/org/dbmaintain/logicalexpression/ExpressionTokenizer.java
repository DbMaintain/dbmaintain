package org.dbmaintain.logicalexpression;

import org.dbmaintain.util.CollectionUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ExpressionTokenizer {

    private static final Set TOKEN_SEPARATORS = CollectionUtils.asSet(" ", "!", "(", ")");

    public List<String> tokenize(String expression) {
        List<String> result = new ArrayList<String>();
        StringBuilder currentToken = new StringBuilder();
        for (char currentChar : expression.toCharArray()) {
            if (isTokenSeparator(currentToken.toString()) || (isTokenSeparator(String.valueOf(currentChar))
                    && currentToken.length() != 0)) {
                result.add(currentToken.toString());
                currentToken = new StringBuilder();
            }
            if (currentChar != ' ')
                currentToken.append(currentChar);
        }
        if (currentToken.length() != 0) result.add(currentToken.toString());
        return result;
    }

    private boolean isTokenSeparator(String token) {
        return TOKEN_SEPARATORS.contains(token);
    }
}
