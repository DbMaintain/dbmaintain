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
package org.dbmaintain.script;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dbmaintain.script.executedscriptinfo.ScriptIndexes;
import org.dbmaintain.script.qualifier.Qualifier;
import org.dbmaintain.util.DbMaintainException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.substringBeforeLast;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ScriptFactory {

    /* The logger instance for this class */
    private static Log logger = LogFactory.getLog(ScriptFactory.class);

    private Pattern scriptIndexPattern;
    private Pattern targetDatabasePattern;
    private Pattern qualifierPattern;
    private Set<Qualifier> registeredQualifiers;
    private Set<Qualifier> patchQualifiers;
    private String preProcessingScriptDirName;
    private String postProcessingScriptDirName;
    /* The baseline revision. If set, all scripts with a lower revision will be ignored */
    protected ScriptIndexes baseLineRevision;


    public ScriptFactory(String scriptIndexRegexp, String targetDatabaseRegexp, String qualifierRegexp, Set<Qualifier> registeredQualifiers, Set<Qualifier> patchQualifiers, String preProcessingScriptDirName, String postProcessingScriptDirName, ScriptIndexes baseLineRevision) {
        this.registeredQualifiers = registeredQualifiers;
        this.patchQualifiers = patchQualifiers;
        this.preProcessingScriptDirName = preProcessingScriptDirName;
        this.postProcessingScriptDirName = postProcessingScriptDirName;
        this.baseLineRevision = baseLineRevision;

        this.scriptIndexPattern = Pattern.compile(scriptIndexRegexp);
        this.targetDatabasePattern = Pattern.compile(targetDatabaseRegexp);
        this.qualifierPattern = Pattern.compile(qualifierRegexp);

        if (baseLineRevision != null) {
            logger.info("The baseline revision is set to " + baseLineRevision.getIndexesString() + ". All script with a lower revision will be ignored");
        }
    }


    public Script createScriptWithContent(String fileName, Long fileLastModifiedAt, ScriptContentHandle scriptContentHandle) {
        return createScript(fileName, fileLastModifiedAt, null, scriptContentHandle);
    }

    public Script createScriptWithoutContent(String fileName, Long fileLastModifiedAt, String checkSum) {
        return createScript(fileName, fileLastModifiedAt, checkSum, null);
    }


    private Script createScript(String fileName, Long fileLastModifiedAt, String checkSum, ScriptContentHandle scriptContentHandle) {
        try {
            String[] pathParts = getPathParts(fileName);
            ScriptIndexes scriptIndexes = getScriptIndexes(pathParts);
            String targetDatabaseName = getTargetDatabaseName(pathParts);
            Set<Qualifier> qualifiers = getQualifiers(pathParts);
            boolean patchScript = isPatchScript(qualifiers);
            boolean preProcessingScript = isPreProcessingScript(fileName);
            boolean postProcessingScript = isPostProcessingScript(fileName);
            boolean ignored = isIgnored(scriptIndexes);

            return new Script(fileName, scriptIndexes, targetDatabaseName, fileLastModifiedAt, checkSum, scriptContentHandle, preProcessingScript, postProcessingScript, patchScript, ignored, qualifiers);

        } catch (DbMaintainException e) {
            throw new DbMaintainException("Error in script " + fileName + ": " + e.getMessage(), e);
        }
    }



    /**
     * @param fileName The script file name, not null
     * @return True if the given script name is a pre processing script
     */
    protected boolean isPreProcessingScript(String fileName) {
    	if (isEmpty(preProcessingScriptDirName)) {
    		return false;
    	}
    	String dirName = preProcessingScriptDirName;
    	if (preProcessingScriptDirName.endsWith("/") || preProcessingScriptDirName.endsWith("\\")) {
    		dirName = preProcessingScriptDirName.substring(0, preProcessingScriptDirName.length() - 1);
    	}
    	return fileName.startsWith(dirName + '/') || fileName.startsWith(dirName + '\\');
    }

    /**
     * @param fileName The script file name, not null
     * @return True if the given script name is a post processing script
     */
    protected boolean isPostProcessingScript(String fileName) {
        if (isEmpty(postProcessingScriptDirName)) {
            return false;
        }
        String dirName = postProcessingScriptDirName;
        if (postProcessingScriptDirName.endsWith("/") || postProcessingScriptDirName.endsWith("\\")) {
            dirName = postProcessingScriptDirName.substring(0, postProcessingScriptDirName.length() - 1);
        }
        return fileName.startsWith(dirName + '/') || fileName.startsWith(dirName + '\\');
    }


    /**
     * Resolves the target database name from the given list of tokens
     * <p>
     * E.g. 01_@databaseA_myscript.sql
     * <p>
     * If the file name consists out of multiple path-parts, the last found target database is used
     * <p>
     * E.g. 01_@database1/01_@database2_myscript.sql
     * <p>
     * will return database2
     *
     * @param pathParts The file path split up in tokens, not null
     * @return The target database name, null if none found
     */
    protected String getTargetDatabaseName(String[] pathParts) {
        List<String> databaseNames = getTokens(pathParts, targetDatabasePattern, false);
        if (databaseNames.isEmpty()) {
            return null;
        }
        return databaseNames.get(databaseNames.size() - 1);
    }

    protected Set<Qualifier> getQualifiers(String[] pathParts) {
        Set<Qualifier> qualifiers = new HashSet<>();

        List<String> qualifierNames = getTokens(pathParts, qualifierPattern, false);

        for (String qualifierName : qualifierNames) {
            Qualifier qualifier = new Qualifier(qualifierName);
            if (!registeredQualifiers.contains(qualifier) && !patchQualifiers.contains(qualifier)) {
                throw new DbMaintainException("Qualifier \"" + qualifier.getQualifierName() + "\" has not been registered.");
            }
            qualifiers.add(qualifier);
        }
        return qualifiers;
    }

    /**
     * Creates a version by extracting the indexes from the the given script file name.
     *
     * @param pathParts The file path split up in tokens, not null
     * @return The version of the script file, not null
     */
    protected ScriptIndexes getScriptIndexes(String[] pathParts) {
        List<Long> versionIndexes = new ArrayList<>();

        List<String> versionIndexStrings = getTokens(pathParts, scriptIndexPattern, true);
        for (String versionIndexString : versionIndexStrings) {
            if (versionIndexString == null) {
                versionIndexes.add(null);
                continue;
            }
            try {
                versionIndexes.add(new Long(versionIndexString));
            } catch (NumberFormatException e) {
                throw new DbMaintainException("Unable to parse version index: " + versionIndexString, e);
            }
        }
        return new ScriptIndexes(versionIndexes);
    }

    protected List<String> getTokens(String[] pathParts, Pattern pattern, boolean addNullIfNoMatch) {
        List<String> tokens = new ArrayList<>();

        for (String pathPart : pathParts) {
            Matcher matcher = pattern.matcher(pathPart);
            boolean foundMatch = matcher.find();
            if (!foundMatch) {
                if (addNullIfNoMatch) {
                    tokens.add(null);
                }
                continue;
            }
            while (foundMatch) {
                String qualifierName = matcher.group(1);
                tokens.add(qualifierName);
                foundMatch = matcher.find();
            }
        }
        return tokens;
    }


    protected String[] getPathParts(String fileName) {
        String fileNameWithoutExtension = substringBeforeLast(fileName, ".");
        String fileNameWithForwardSlashes = fileNameWithoutExtension.replace('\\', '/');
        return StringUtils.split(fileNameWithForwardSlashes, '/');
    }


    /**
     * @param scriptIndexes The revision of the script, not null
     * @return True if a baseline revision was set and the revision of the script was below the baseline
     */
    protected boolean isIgnored(ScriptIndexes scriptIndexes) {
        return baseLineRevision != null && baseLineRevision.compareTo(scriptIndexes) > 0;
    }

    /**
     * @param qualifiers a set of script qualifiers, not null
     * @return True if there is a patch qualifier
     */
    public boolean isPatchScript(Set<Qualifier> qualifiers) {
        for (Qualifier qualifier : qualifiers) {
            if (patchQualifiers.contains(qualifier)) {
                return true;
            }
        }
        return false;
    }


}
