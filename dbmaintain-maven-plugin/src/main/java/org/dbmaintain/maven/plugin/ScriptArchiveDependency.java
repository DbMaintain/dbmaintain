package org.dbmaintain.maven.plugin;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ScriptArchiveDependency {

    /**
     * @parameter
     * @required
     */
    protected String groupId;
    /**
     * @parameter
     * @required
     */
    protected String artifactId;
    /**
     * @parameter
     * @required
     */
    protected String version;
    /**
     * @parameter default-value="jar"
     */
    protected String type = "jar";
    /**
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
