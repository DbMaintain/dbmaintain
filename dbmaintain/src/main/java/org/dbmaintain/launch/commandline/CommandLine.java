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

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.dbmaintain.config.PropertiesDbMaintainConfigurer;
import org.dbmaintain.dbsupport.DefaultSQLHandler;
import org.dbmaintain.launch.DbMaintain;
import org.dbmaintain.util.DbMaintainConfigurationLoader;
import org.dbmaintain.util.FileUtils;

/**
 * Class that exposes a set of DbMaintain operations for command line execution.
 * 
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class CommandLine {

    /**
     * Enum that defines all DbMaintain operations that can be invoked using this class.  
     */
    public enum DbMaintainOperation {
        
        CREATE_DB_JAR("createJar"), 
        UPDATE_DATABASE("update"), 
        MARK_DATABASE_AS_UPTODATE("markAsUptodate"), 
        CLEAR_DATABASE("clear"), 
        CLEAN_DATABASE("clean"),
        DISABLE_CONSTRAINTS("disableConstraints"), 
        UPDATE_SEQUENCES("updateSequences");
        
        private String operationName;
        
        private DbMaintainOperation(String operationName) {
            this.operationName = operationName;
        }
        
        /**
         * @return The name of the operation, that can be used as first command line argument to invoke an operation
         */
        public String getOperationName() {
            return operationName;
        }

        /**
         * @param operationName The name of the operation, that can be used as first command line argument to invoke an operation
         * 
         * @return The operation identified by the given operation name 
         */
        public static DbMaintainOperation getByOperationName(String operationName) {
            for (DbMaintainOperation operation : values()) {
                if (operation.getOperationName().equalsIgnoreCase(operationName)) {
                    return operation;
                }
            }
            return null;
        }
    }
    
    
    /**
     * Executes a DbMaintain operation. The first command-line argument defines the operation that
     * must be executed. The second argument defines the properties file that is used to configure
     * DbMaintain.
     * 
     * @param args The command line arguments
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("At least 2 arguments expected");
            printHelpMessage();
            System.exit(1);
        }
        DbMaintainOperation operation = DbMaintainOperation.getByOperationName(args[0]);
        if (operation == null) {
            System.err.println("Invalid operation " + args[0]);
            printHelpMessage();
            System.exit(1);
        }
        URL propertiesAsURL = getPropertiesAsURL(args[1]);
        if (propertiesAsURL == null) {
            System.err.println("Could not find properties file" + args[1]);
            System.exit(1);
        }
        Properties configuration = new DbMaintainConfigurationLoader().loadConfiguration(propertiesAsURL);
        executeOperation(operation, configuration, args);
    }

    /**
     * Executes the given operation using the given configuration.
     * 
     * @param operation The operation that must be executed
     * @param configuration The dbMaintain configuration
     * @param args The command line arguments
     */
    public static void executeOperation(DbMaintainOperation operation, Properties configuration, String args[]) {
        PropertiesDbMaintainConfigurer dbMaintainConfigurer = new PropertiesDbMaintainConfigurer(configuration, new DefaultSQLHandler());
        DbMaintain dbMaintain = new DbMaintain(dbMaintainConfigurer);
        
        switch (operation) {
        case CREATE_DB_JAR:
            if (args.length < 3) {
                System.err.println("Jar file name must be specified as third argument");
                System.exit(1);
            }
            String jarFileName = args[2];
            dbMaintain.createDbJar(jarFileName);
            break;
        case UPDATE_DATABASE:
            dbMaintain.updateDatabase();
            break;
        case MARK_DATABASE_AS_UPTODATE:
            dbMaintain.markDatabaseAsUptodate();
            break;
        case CLEAR_DATABASE:
            dbMaintain.clearDatabase();
            break;
        case CLEAN_DATABASE:
            dbMaintain.cleanDatabase();
            break;
        case DISABLE_CONSTRAINTS:
            dbMaintain.disableConstraints();
            break;
        case UPDATE_SEQUENCES:
            dbMaintain.updateSequences();
            break;
        }
    }
    
    
    /**
     * @param fileName The name of the file
     * 
     * @return An inputStream giving access to the file with the given name.
     */
    private static URL getPropertiesAsURL(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            return null;
        }
        return FileUtils.getUrl(file);
    }

    
    /**
     * Prints out a help message that explains the usage of this class
     */
    public static void printHelpMessage() {
        System.out.println();
        System.out.println("Usage:");
        System.out.println();
        System.out.println("java org.dbmaintain.launch.DbMaintain operation propertiesFile [jarFile]");
        System.out.println("Available operations are:");
        System.out.println();
        System.out.println("- " + DbMaintainOperation.CREATE_DB_JAR.getOperationName());
        System.out.println("     Expects a third argument indicating the jar file name.");
        System.out.println("     Creates a jar file containing all scripts in all configured script locations.");
        System.out.println();
        System.out.println("- " + DbMaintainOperation.UPDATE_DATABASE.getOperationName());
        System.out.println("     Updates the database to the latest version.");
        System.out.println();
        System.out.println("- " + DbMaintainOperation.MARK_DATABASE_AS_UPTODATE.getOperationName());
        System.out.println("     Marks the database as up-to-date, without executing any script. " +
        		"You can use this operation to prepare an existing database to be managed by DbMaintain, or after having manually fixed a problem.");
        System.out.println("- " + DbMaintainOperation.CLEAR_DATABASE.getOperationName());
        System.out.println("     Removes all database items, and empties the DBMAINTAIN_SCRIPTS table.");
        System.out.println("- " + DbMaintainOperation.CLEAN_DATABASE.getOperationName());
        System.out.println("     Removes the data of all database tables, except for the DBMAINTAIN_SCRIPTS table.");
        System.out.println("- " + DbMaintainOperation.DISABLE_CONSTRAINTS.getOperationName());
        System.out.println("     Disables or drops all foreign key and not null constraints.");
        System.out.println("- " + DbMaintainOperation.UPDATE_SEQUENCES.getOperationName());
        System.out.println("     Updates all sequences and identity columns to a minimum value.");
    }

}
