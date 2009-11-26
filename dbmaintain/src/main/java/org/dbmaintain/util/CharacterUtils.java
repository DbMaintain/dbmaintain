package org.dbmaintain.util;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CharacterUtils {
    
    private static final Character CARRIAGE_RETURN = '\r', NEW_LINE = '\n';

    public static boolean isNewLineCharacter(Character currentChar) {
        return CARRIAGE_RETURN.equals(currentChar) || NEW_LINE.equals(currentChar);
    }
}
