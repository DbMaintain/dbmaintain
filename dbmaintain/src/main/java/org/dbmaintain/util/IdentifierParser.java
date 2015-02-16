/*
 * Copyright DbMaintain.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.        

 */

package org.dbmaintain.util;

import java.util.List;
import java.util.ArrayList;

/**
 * @author Christian Liebhardt
 */
public class IdentifierParser {
    
    private ParsingState currentState;
    private List<String> parts = new ArrayList<>();
    private char separatorChar;
    private String part = "";
    public IdentifierParser(char separatorChar) {
        this.separatorChar = separatorChar;
    }

    private interface ParsingState {
        public void process(char c);
    }
    private class NormalState implements ParsingState {
        public void process(char c) {
            if (c == '\'') {
                part += c;
                currentState = new SingleQuotedState();
            }
            else if (c== '"') {
                part += c;
                currentState = new DoubleQuotedState();
            }
            else if (c == separatorChar) {
                parts.add(part);
                part = "";
            }
            else {
                part += c;
            }
        }
    }
    private class DoubleQuotedState implements ParsingState {
        public void process(char c) {
            part += c;
            if (c == '"') {
                currentState = new NormalState();
            }
        }
    }
    private class SingleQuotedState implements ParsingState {
        public void process(char c) {
            part += c;
            if (c == '\'') {
                currentState = new NormalState();
            }
        }
    }

    public String[] parse(String identifierAsString) {
        currentState =  new NormalState();
        for (int i = 0; i < identifierAsString.length(); i++) {
            char c = identifierAsString.charAt(i);
            currentState.process(c);
        }
        if (part.length() != 0) {
            parts.add(part);
        }
        return parts.toArray(new String[0]);
    }
}
