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
package org.dbmaintain.script;

import java.util.Date;

/**
 * @author Filip Neven
 * @author Tim Ducheyne
 */
public class ExecutedScript implements Comparable<ExecutedScript> {

	private Script script;
	
	private Date executedAt;
	
	private Boolean successful;

	public ExecutedScript(Script script, Date executedAt, Boolean successful) {
		this.script = script;
		this.executedAt = executedAt;
		this.successful = successful;
	}

	
	public Script getScript() {
		return script;
	}

	
	public Date getExecutedAt() {
		return executedAt;
	}

	
	public Boolean isSuccessful() {
		return successful;
	}


	public void setSuccessful(Boolean successful) {
		this.successful = successful;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutedScript)) return false;

        ExecutedScript that = (ExecutedScript) o;

        if (!script.equals(that.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    @Override
    public String toString() {
        return script.getFileName();
    }


    public int compareTo(ExecutedScript other) {
        return script.compareTo(other.getScript());
    }
}
