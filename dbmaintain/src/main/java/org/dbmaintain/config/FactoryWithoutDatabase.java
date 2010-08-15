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

import java.util.Properties;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public abstract class FactoryWithoutDatabase<T> implements Factory<T> {

    protected FactoryContext factoryContext;


    public void init(FactoryContext factoryContext) {
        this.factoryContext = factoryContext;
    }

    public Properties getConfiguration() {
        return factoryContext.getConfiguration();
    }

}
