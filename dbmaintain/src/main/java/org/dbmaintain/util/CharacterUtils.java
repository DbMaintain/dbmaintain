package org.dbmaintain.util;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CharacterUtils {

    public static boolean isNewLineCharacter(Character currentChar) {
        return currentChar == '\n' || currentChar == '\r';
    }
}
