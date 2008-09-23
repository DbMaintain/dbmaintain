/*
 * Copyright 2007 www.codespot.net
 * Created by Alexander Snaps <alex.snaps@gmail.com>
 * on Jun 23, 2008 - 8:53:04 AM
 *
 * $Id$
 */
package org.dbmaintain.util;

import org.apache.tools.ant.util.ReaderInputStream;
import org.dbmaintain.script.Script;
import org.dbmaintain.script.impl.DefaultScriptSource;
import org.dbmaintain.thirdparty.org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Enables creating a jar file that packages all database update scripts. This jar can be used by the
 * {@link DbScriptJarRunner} to apply changes incrementally on a target database. This way, database updates
 * can be distributed in the form of a deliverable, just like a war, ear or par file. 
 * 
 * The jar file that's created contains all configuration that concerns the organization of the scripts in this 
 * jar in the properties file {@link #DBSCRIPT_JAR_PROPERTIES_FILENAME}
 * 
 * This class can optionally be initialized with an existing configuration file that defines the organization of 
 * the scripts for the project (configurationFile constructor param). Typically, this is the file unitils.properties. 
 * The values of these properties can be overridden by directly intializing these values on this class. The properties 
 * that are retrieved from this file are the scripts location (property {@link DefaultScriptSource.PROPKEY_SCRIPTS_LOCATION}, 
 * overridable using the location constructor param), the script file extensions (property {@link DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS}, 
 * overridable using the extensions constructor param) and the start of the name of directories that contain post 
 * processing scripts (property {@link DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME}, overridable 
 * using the postProcessingDirNameStartsWith constructor param).  
 * 
 * @author Alexander Snaps <alex.snaps@gmail.com>
 * @author Filip Neven
 */
public class DbScriptJarCreator {

	/*
	 * Name of the properties file that is packaged with the jar, that contains information about how
	 * the scripts in the jar file are structured. 
	 */
    public static final String DBSCRIPT_JAR_PROPERTIES_FILENAME = "META-INF/dbscriptjar.properties";
	
    /*
     * If not null, overrides the property DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME 
     */
    private String postProcessingDirName;
    
    /*
     * If not null, overrides the property DefaultScriptSource.PROPKEY_SCRIPTS_LOCATION
     */
    private String location;
    
    /*
     * If not null, overrides the property DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS
     */
    private String extensions;
    
    private String encoding;
    
    private String targetDatabasePrefix;
    
    private Properties defaultConfiguration = new ConfigurationLoader().getDefaultConfiguration();

    
    /**
     * Creates an instance
     * 
     * @param location Must not be null
     * @param extensions If not null, overrides the property DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS
     * @param postProcessingDirName If not null, overrides the property DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME
     * @param encoding If not null, overrides the property DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS
     * @param targetDatabasePrefix If not null, overrides the property DefaultScriptSource.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX
     */
	public DbScriptJarCreator(String location, String extensions, String postProcessingDirName, String encoding, String targetDatabasePrefix) {
        this.location = location;
        this.extensions = extensions;
        this.postProcessingDirName = postProcessingDirName;
        this.encoding = encoding;
        this.targetDatabasePrefix = targetDatabasePrefix;
    }
	
	
	/**
	 * Creates the jar containing the scripts and stores it in the file with the given file name
	 * 
	 * @param jarFilename Path where the jar file is stored
	 */
    public void createJar(String jarFilename) {
    	JarOutputStream jarOutputStream = null;
        
        try {
			Properties configuration = getConfigurationToIncludeInJar();
			List<Script> scripts = getScripts();
			File jarFile = new File(jarFilename);
			jarOutputStream = new JarOutputStream(new FileOutputStream(jarFile));
			Writer propertiesFileWriter = new StringWriter();
			configuration.store(new WriterOutputStream(propertiesFileWriter), null);
			writeJarEntry(jarOutputStream, DBSCRIPT_JAR_PROPERTIES_FILENAME,
					new StringReader(propertiesFileWriter.toString()));
			for (Script script : scripts) {
				writeJarEntry(jarOutputStream, script.getFileName(), script
						.getScriptContentHandle().openScriptContentReader());
			}
        } catch (IOException e) {
        	throw new DbMaintainException("Error while creating jar file " + jarFilename, e);
		} finally {
			IOUtils.closeQuietly(jarOutputStream);
		}
    }

    
    /**
     * Writes the entry with the given name and content to the given {@link JarOutputStream}
     * 
     * @param jarOutputStream {@link OutputStream} to the jar file
     * @param entryName Name of the jar file entry
     * @param entryContentReader Reader giving access to the content of the jar entry
     * @throws IOException In case of disk IO problems
     */
	private void writeJarEntry(JarOutputStream jarOutputStream, String entryName, Reader entryContentReader)
			throws IOException {
		JarEntry jarEntry = new JarEntry(entryName);
		jarOutputStream.putNextEntry(jarEntry);

		InputStream scriptInputStream = new ReaderInputStream(entryContentReader, encoding != null ?
		        encoding : defaultConfiguration.getProperty(DefaultScriptSource.PROPKEY_SCRIPTS_ENCODING));
		byte[] buffer = new byte[1024];
		int len;
		while((len = scriptInputStream.read(buffer, 0, buffer.length)) > -1) {
		    jarOutputStream.write(buffer, 0, len);
		}
		scriptInputStream.close();
		jarOutputStream.closeEntry();
	}

	
	/**
	 * @return All database update scripts, in the sequence in which they must be executed
	 */
    private List<Script> getScripts() {
        List<Script> allScripts = new ArrayList<Script>();
        
        DefaultScriptSource source = new DefaultScriptSource();
        source.init(getScriptSourceConfiguration());
        allScripts.addAll(source.getAllUpdateScripts());
        allScripts.addAll(source.getPostProcessingScripts());
        
        return allScripts;
    }

    
    /**
     * @return All properties that provide information about the organization of the scripts in the jar file. These properties are included in the 
     * jar file, in the file {@link #DBSCRIPT_JAR_PROPERTIES_FILENAME}
     */
    private Properties getConfigurationToIncludeInJar() {
    	Properties conf = new Properties();
		if (extensions != null) {
			conf.put(DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS, extensions);
		}
		if (postProcessingDirName != null) {
            conf.put(DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingDirName);
        }
		if (encoding != null) {
		    conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_ENCODING, encoding);
		}
		if (targetDatabasePrefix != null) {
            conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        }
		return conf;
	}
    
    
    /**
     * @return All properties that are required by the {@link DefaultScriptSource}
     */
    private Properties getScriptSourceConfiguration() {
		Properties conf = new Properties();
		conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_LOCATION, location);
		if (extensions != null) {
			conf.put(DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS, extensions);
		} else {
		    conf.put(DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS, 
		            defaultConfiguration.getProperty(DefaultScriptSource.PROPKEY_SCRIPT_EXTENSIONS));
		}
		if (postProcessingDirName != null) {
			conf.put(DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, postProcessingDirName);
		} else {
		    conf.put(DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME, 
                    defaultConfiguration.getProperty(DefaultScriptSource.PROPKEY_POSTPROCESSINGSCRIPTS_DIRNAME));
		}
		if (encoding != null) {
		    conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_ENCODING, encoding);
		} else {
		    conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_ENCODING, 
                    defaultConfiguration.getProperty(DefaultScriptSource.PROPKEY_SCRIPTS_ENCODING));
		}
		if (targetDatabasePrefix != null) {
            conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, targetDatabasePrefix);
        } else {
            conf.put(DefaultScriptSource.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX, 
                    defaultConfiguration.getProperty(DefaultScriptSource.PROPKEY_SCRIPTS_TARGETDATABASE_PREFIX));
        }
		return conf;
	}

}
