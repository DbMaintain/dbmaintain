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

import org.dbmaintain.launch.task.DbMaintainDatabase;
import org.dbmaintain.launch.task.DbMaintainTask;
import org.dbmaintain.launch.task.UpdateSequencesTask;

import java.util.List;

/**
 * This operation is also mainly useful for automated testing purposes. This operation sets all sequences and identity
 * columns to a minimum value. By default this value is 1000, but is can be configured with the
 * lowestAcceptableSequenceValue option. The updateDatabase operation offers an option to automatically update the
 * sequences after the scripts were executed.
 *
 * @author Tim Ducheyne
 * @author tiwe
 * @goal updateSequences
 */
public class UpdateSequencesMojo extends BaseDatabaseMojo {

    /**
     * Threshold indicating the minimum value of sequences. If sequences are updated, all sequences having a lower value than this
     * one are set to this value. Defaults to 1000.
     *
     * @parameter
     */
    protected Long lowestAcceptableSequenceValue;


    @Override
    protected DbMaintainTask createDbMaintainTask(List<DbMaintainDatabase> dbMaintainDatabases) {
        return new UpdateSequencesTask(dbMaintainDatabases, lowestAcceptableSequenceValue);
    }
}
