package org.dbmaintain.structure.sequence;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.config.PropertyUtils;
import org.dbmaintain.structure.sequence.impl.DefaultSequenceUpdater;

import static org.dbmaintain.config.DbMaintainProperties.PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class SequenceUpdaterFactory extends FactoryWithDatabase<DefaultSequenceUpdater> {


    public DefaultSequenceUpdater createInstance() {
        long lowestAcceptableSequenceValue = PropertyUtils.getLong(PROPERTY_LOWEST_ACCEPTABLE_SEQUENCE_VALUE, getConfiguration());
        return new DefaultSequenceUpdater(lowestAcceptableSequenceValue, getDatabases());
    }
}
