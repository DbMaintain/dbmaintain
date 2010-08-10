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
