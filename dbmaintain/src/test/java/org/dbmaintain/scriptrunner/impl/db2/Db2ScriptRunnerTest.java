package org.dbmaintain.scriptrunner.impl.db2;

import org.dbmaintain.dbsupport.DatabaseInfo;
import org.dbmaintain.dbsupport.impl.Db2DbSupport;
import org.dbmaintain.dbsupport.impl.DefaultSQLHandler;
import org.dbmaintain.script.Script;
import org.dbmaintain.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

import static java.util.Arrays.asList;
import static org.dbmaintain.dbsupport.DbMaintainDataSource.createDataSource;
import static org.dbmaintain.dbsupport.StoredIdentifierCase.UPPER_CASE;
import static org.dbmaintain.util.TestUtils.getNameDbSupportMap;


public class Db2ScriptRunnerTest {

    private Db2ScriptRunner db2ScriptRunner;

    @Test
    public void dummy(){
        
    }

    //@Before
    public void initialize() {
        DatabaseInfo databaseInfo = new DatabaseInfo(null, null, "com.ibm.db2.jcc.DB2Driver", "jdbc:db2://localhost:50000/TEST_DB2", "admin", "admin", asList("TEST_DB2"));
        DataSource dataSource = createDataSource(databaseInfo);
        Db2DbSupport db2DbSupport = new Db2DbSupport(databaseInfo, dataSource, new DefaultSQLHandler(), "\"", UPPER_CASE);

        db2ScriptRunner = new Db2ScriptRunner(db2DbSupport, getNameDbSupportMap(db2DbSupport), "db2");

    }

    //@Test
    public void runScript() {
        Script script1 = TestUtils.createScript("test1.sql", "CREATE TABLE TEST (ID BIGINT);");
        Script script2 = TestUtils.createScript("test2.sql", "DROP TABLE TEST;");

        db2ScriptRunner.initialize();
        db2ScriptRunner.execute(script1);
        db2ScriptRunner.execute(script2);
        db2ScriptRunner.close();
    }
}
