/*
 * Copyright 2006-2007,  Unitils.org
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
package org.dbmaintain.launch.commandline;

import org.dbmaintain.util.DbMaintainException;


/**
 * Data object that exposes the command line arguments that were passed to DbMaintain
 *  
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CommandLineArguments {
    
    private String dbMaintainOperation;
    private String firstExtraArgument, secondExtraArgument;
    private String configFile;
    
    public CommandLineArguments(String[] commandLineArgs) {
        parseArguments(commandLineArgs);
    }

    protected void parseArguments(String[] commandLineArgs) {
        boolean nextArgumentIsConfigFile = false;
        for (String commandLineArg : commandLineArgs) {
            if (nextArgumentIsConfigFile) {
                configFile = commandLineArg;
                continue;
            }
            if ("-config".equals(commandLineArg)) {
                nextArgumentIsConfigFile = true;
                continue;
            }
            if (commandLineArg.startsWith("-")) {
                throw new DbMaintainException("Invalid command line option " + commandLineArg);
            }
            if (dbMaintainOperation == null) {
                dbMaintainOperation = commandLineArg;
                continue;
            }
            if (firstExtraArgument == null) {
                firstExtraArgument = commandLineArg;
                continue;
            }
            if (secondExtraArgument == null) {
                secondExtraArgument = commandLineArg;
                continue;
            }
        }
        if (dbMaintainOperation == null) {
            throw new DbMaintainException("No operation was specified");
        }
    }

    public String getDbMaintainOperation() {
        return dbMaintainOperation;
    }

    public String getFirstExtraArgument() {
        return firstExtraArgument;
    }

    public String getSecondExtraArgument() {
        return secondExtraArgument;
    }

    public String getConfigFile() {
        return configFile;
    }

}
