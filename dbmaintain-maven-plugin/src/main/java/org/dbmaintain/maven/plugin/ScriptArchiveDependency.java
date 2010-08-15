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
package org.dbmaintain.maven.plugin;

/**
 * A scripts-archive maven dependency.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ScriptArchiveDependency {

    /**
     * The artifact group id.
     *
     * @parameter
     * @required
     */
    protected String groupId;
    /**
     * The artifact id.
     *
     * @parameter
     * @required
     */
    protected String artifactId;
    /**
     * The artifact version.
     *
     * @parameter
     * @required
     */
    protected String version;
    /**
     * The artifact type. Defaults to jar.
     *
     * @parameter default-value="jar"
     */
    protected String type = "jar";
    /**
     * The artifact classifier. Defaults to no classifier.
     *
     * @parameter
     */
    protected String classifier;


    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public String getType() {
        return type;
    }

    public String getClassifier() {
        return classifier;
    }
}
