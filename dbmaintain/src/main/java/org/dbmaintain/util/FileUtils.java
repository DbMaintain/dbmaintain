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
package org.dbmaintain.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;


// todo javadoc
public class FileUtils {


    /**
     * Creates an URL that points to the given file.
     *
     * @param file The file, not null
     * @return The URL to the file, not null
     */
    public static URL getUrl(File file) {
        try {
            // The file is first converted to an URI and then to an URL, since this way characters
            // that are illegal in an URL are automatically escaped.
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new DbMaintainException("Unable to create URL for file " + file.getName(), e);
        }
    }
}
