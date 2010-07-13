package org.dbmaintain.structure;

import org.dbmaintain.config.FactoryWithDatabase;
import org.dbmaintain.structure.impl.DefaultConstraintsDisabler;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public class ConstraintsDisablerFactory extends FactoryWithDatabase<ConstraintsDisabler> {


    public ConstraintsDisabler createInstance() {
        return new DefaultConstraintsDisabler(getDbSupports());
    }
}
