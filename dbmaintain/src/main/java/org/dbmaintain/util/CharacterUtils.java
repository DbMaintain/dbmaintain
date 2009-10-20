package org.dbmaintain.util;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CharacterUtils {
    private static final Character CARRIAGE_RETURN = '\r', LINE_FEED = '\n';

    public static boolean isNewLineCharacter(Character currentChar) {
        return CARRIAGE_RETURN.equals(currentChar) || LINE_FEED.equals(currentChar);
    }
}
