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
package org.dbmaintain.script.runner.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.util.DbMaintainException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class Application {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(Application.class);

    private String name;
    private String command;
    private Map<String, String> environmentVariables;


    public Application(String name, String command) {
        this(name, command, new HashMap<>());
    }

    public Application(String name, String command, Map<String, String> environmentVariables) {
        this.name = name;
        this.command = command;
        this.environmentVariables = environmentVariables;
    }


    public ProcessOutput execute(String... arguments) {
        return execute(true, arguments);
    }

    public ProcessOutput execute(boolean logCommand, String... arguments) {
        try {
            List<String> commandWithArguments = getProcessArguments(arguments);

            ProcessBuilder processBuilder = createProcessBuilder(commandWithArguments);
            Process process = processBuilder.start();
            OutputProcessor outputProcessor = new OutputProcessor(process);
            outputProcessor.start();
            process.waitFor();

            String output = outputProcessor.getOutput();
            int exitValue = process.exitValue();

            logOutput(commandWithArguments, output, logCommand);
            return new ProcessOutput(output, exitValue);

        } catch (Exception e) {
        	throw new DbMaintainException("Failed to execute command: " + command + " " + e.getMessage(), e);
        }
    }

    protected void logOutput(List<String> commandWithArguments, String output, boolean logCommand) {
        StringBuilder command = new StringBuilder();
        if (logCommand) {
            for (String part : commandWithArguments) {
                command.append(part);
                command.append(" ");
            }
        }
        logger.debug(name + ": " + command + "\n" + output);
    }


    protected ProcessBuilder createProcessBuilder(List<String> commandWithArguments) {
        ProcessBuilder processBuilder = new ProcessBuilder(commandWithArguments);
        Map<String, String> processEnvironment = processBuilder.environment();
        processEnvironment.putAll(environmentVariables);
        processBuilder.redirectErrorStream(true);
        return processBuilder;
    }

    protected List<String> getProcessArguments(String[] arguments) {
        List<String> commandWithArguments = new ArrayList<>();
        commandWithArguments.add(command);
        commandWithArguments.addAll(asList(arguments));
        return commandWithArguments;
    }


    public static class ProcessOutput {

        private String output;
        private int exitValue;

        public ProcessOutput(String output, int exitValue) {
            this.output = output;
            this.exitValue = exitValue;
        }

        public String getOutput() {
            return output;
        }

        public int getExitValue() {
            return exitValue;
        }
    }


    protected class OutputProcessor extends Thread {

        private StringBuilder outputStringBuilder = new StringBuilder();
        private Process process;

        public OutputProcessor(Process process) {
            this.process = process;
        }

        @Override
        public void run() {
            try {
                appendProcessOutput(process);

            } catch (Throwable t) {
                logger.warn("Unable to handle application output for " + command, t);
            }
        }

        public String getOutput() {
            return outputStringBuilder.toString();
        }

        protected void appendProcessOutput(Process process) throws IOException {
            try (BufferedReader outReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = outReader.readLine()) != null) {
                    if (!isBlank(line)) {
                        outputStringBuilder.append(line);
                        outputStringBuilder.append('\n');
                    }
                }
            }
        }
    }
}
