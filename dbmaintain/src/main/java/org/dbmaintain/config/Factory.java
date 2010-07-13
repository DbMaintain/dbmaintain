package org.dbmaintain.config;

/**
 * @author Tim Ducheyne
 * @author Filip Neven
 */
public interface Factory<T> {

    T createInstance();

}
