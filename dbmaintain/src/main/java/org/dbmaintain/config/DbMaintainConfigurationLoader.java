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
package org.dbmaintain.config;

import org.dbmaintain.util.DbMaintainException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static org.apache.commons.io.IOUtils.closeQuietly;


/**
 * Utility that loads the configuration of DbMaintain.
 *
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class DbMaintainConfigurationLoader {

    /**
     * Name of the fixed configuration file that contains all defaults
     */
    public static final String DEFAULT_PROPERTIES_FILE_NAME = "dbmaintain-default.properties";


    /**
     * Loads all properties as defined by the default configuration.
     *
     * @return the settings, not null
     */
    public Properties loadConfiguration() {
        return loadConfiguration((Properties) null);
    }


    /**
     * Loads all properties as defined by the default configuration. Properties defined by the properties
     * file to which the given URL points override the default properties.
     *
     * @param customConfigurationUrl URL that points to the custom configuration, may be null if there is no custom config
     * @return the settings, not null
     */
    public Properties loadConfiguration(URL customConfigurationUrl) {
        return loadConfiguration(loadPropertiesFromURL(customConfigurationUrl));
    }

    /**
     * Loads all properties as defined by the default configuration. Properties defined by the properties
     * file to which the given URL points override the default properties.
     *
     * @param customConfigurationFile The custom configuration, may be null if there is no custom config
     * @return the settings, not null
     */
    public Properties loadConfiguration(File customConfigurationFile) {
        return loadConfiguration(loadPropertiesFromFile(customConfigurationFile));
    }


    /**
     * Loads all properties as defined by the default configuration. Properties from the given properties
     * object override the default properties.
     *
     * @param customConfiguration custom configuration, may be null if there is no custom config
     * @return the settings, not null
     */
    public Properties loadConfiguration(Properties customConfiguration) {
        Properties properties = new Properties();

        // Load the default properties file, that is distributed with DbMaintain (dbmaintain-default.properties)
        properties.putAll(loadDefaultConfiguration());

        if (customConfiguration != null) {
            properties.putAll(customConfiguration);
        }
        return properties;
    }


    /**
     * Creates and loads the default configuration settings from the {@link #DEFAULT_PROPERTIES_FILE_NAME} file.
     *
     * @return the defaults, not null
     * @throws RuntimeException if the file cannot be found or loaded
     */
    public Properties loadDefaultConfiguration() {
        Properties defaultConfiguration = loadPropertiesFromClasspath(DEFAULT_PROPERTIES_FILE_NAME);
        if (defaultConfiguration == null) {
            throw new DbMaintainException("Configuration file: " + DEFAULT_PROPERTIES_FILE_NAME + " not found in classpath.");
        }
        return defaultConfiguration;
    }


    protected Properties loadPropertiesFromClasspath(String propertiesFileName) {
        InputStream inputStream = null;
        try {
            inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName);
            if (inputStream == null) {
                return null;
            }
            return loadPropertiesFromStream(inputStream);

        } catch (IOException e) {
            throw new DbMaintainException("Unable to load configuration file: " + propertiesFileName, e);
        } finally {
            closeQuietly(inputStream);
        }
    }

    protected Properties loadPropertiesFromURL(URL propertiesFileUrl) {
        if (propertiesFileUrl == null) {
            return null;
        }
        InputStream urlStream = null;
        try {
            urlStream = propertiesFileUrl.openStream();
            return loadPropertiesFromStream(urlStream);
        } catch (IOException e) {
            throw new DbMaintainException("Unable to load configuration file " + propertiesFileUrl, e);
        } finally {
            closeQuietly(urlStream);
        }
    }

    protected Properties loadPropertiesFromFile(File propertiesFile) {
        if (propertiesFile == null) {
            return null;
        }
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(propertiesFile);
            return loadPropertiesFromStream(inputStream);
        } catch (IOException e) {
            throw new DbMaintainException("Unable to load configuration file " + propertiesFile, e);
        } finally {
            closeQuietly(inputStream);
        }
    }


    protected Properties loadPropertiesFromStream(InputStream inputStream) throws IOException {
        Properties properties = new Properties();
        properties.load(inputStream);
        return properties;
    }

}
