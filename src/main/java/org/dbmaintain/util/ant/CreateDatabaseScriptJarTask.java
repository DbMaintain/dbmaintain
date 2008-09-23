/*
 * Copyright 2006-2008,  Unitils.org
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
 *
 * $Id$
 */
package org.dbmaintain.util.ant;

import org.dbmaintain.util.DbScriptJarCreator;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

/**
 * @author Alexander Snaps <alex.snaps@gmail.com>
 * @author Filip Neven
 */
public class CreateDatabaseScriptJarTask extends Task {

    private String jarFileName;
    
    private String location;
    private String extensions;
    private String postProcessingDirName;
    private String encoding;
    private String targetDatabasePrefix;

    public void execute()
            throws BuildException {

        try {
			DbScriptJarCreator creator = new DbScriptJarCreator(location, extensions, postProcessingDirName, encoding, targetDatabasePrefix);
            creator.createJar(jarFileName);
        } catch(Exception e) {
        	e.printStackTrace();
            throw new BuildException("Error creating jar file " + jarFileName, e);
        }
    }

    public void setJarFileName(String jarFileName) {
        this.jarFileName = jarFileName;
    }

	public void setLocation(String location) {
        this.location = location;
    }

    public void setExtensions(String extensions) {
        this.extensions = extensions;
    }

    public void setPostProcessingDirName(String postProcessingDirName) {
        this.postProcessingDirName = postProcessingDirName;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
    
    public void setTargetDatabasePrefix(String targetDatabasePrefix) {
        this.targetDatabasePrefix = targetDatabasePrefix;
    }
}
