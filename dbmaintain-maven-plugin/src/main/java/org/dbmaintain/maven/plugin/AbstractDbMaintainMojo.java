package org.dbmaintain.maven.plugin;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.dbmaintain.launch.DbMaintain;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Properties;

/**
 * @author tiwe
 */
public abstract class AbstractDbMaintainMojo extends AbstractMojo {

    /**
     * The DbMaintain configuration file
     * (common for native dbMaintain, through ant or this maven-plugin).
     *
     * @parameter expression="${dbmaintain.configFile}" default-value="dbmaintain.properties"
     * @required
     */
    private File configFile;

    /**
     * Optional set of extra properties which will override any from {@linkplain #configFile}.
     *
     * @parameter
     */
    private Map<String, String> properties;


    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configFile));

            if (this.properties != null) {
                // additional props
                for (Map.Entry<String, String> entry : this.properties.entrySet()) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }

            // sys props
            for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
                if (entry.getKey().toString().startsWith("database.")) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
            execute(new DbMaintain(properties, usesDatabase()));
        }
        catch (MalformedURLException e) {
            throw new MojoFailureException(e.getMessage(), e);
        }
        catch (FileNotFoundException e) {
            throw new MojoFailureException(e.getMessage(), e);
        }
        catch (IOException e) {
            throw new MojoFailureException(e.getMessage(), e);
        }
    }

    protected abstract void execute(DbMaintain dbMaintain);


    /**
     * @return True if a connection to the database is needed, false otherwise. Defaults to true.
     */
    protected boolean usesDatabase() {
        return true;
    }

}
