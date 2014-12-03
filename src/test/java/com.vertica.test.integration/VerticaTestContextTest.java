package com.vertica.test.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class VerticaTestContextTest {

    @Before
    public void setUp() {
        context = new TestVerticaTestContext();
        context.setup();
    }

    @After
    public void tearDown() {
        context.cleanup();
    }

    @Test
    public void createsAndDropsSchemaAndCreatesUserForCreatedSchema() throws SQLException {
        ResultSet result = context.createStatement().executeQuery("SHOW SEARCH_PATH");
        assertTrue(result.next());

        String schemaName = context.getDatabase().getSchema().getSchemaName();
        assertTrue(result.getString("setting").contains(schemaName));
    }

    @Test
    public void createsTable() throws SQLException {

        context.execute("CREATE TABLE test_table (column_name INT)");

        String name = context.getDatabase().getSchema().getSchemaName();
        String validationQuery = "SELECT * FROM TABLES WHERE table_schema='" + name + "' AND table_name='test_table'";
        ResultSet result = context.createStatement().executeQuery(validationQuery);
        assertTrue(result.next());
    }

    @Test
    public void createsFewTablesFromSqlFile() throws SQLException, FileNotFoundException {

        context.execute(getFile("/test_create_tables.sql"));

        String name = context.getDatabase().getSchema().getSchemaName();
        String validationQuery1 = "SELECT * FROM TABLES WHERE table_schema='" + name + "' AND table_name='test_table_1'";
        ResultSet result1 = context.createStatement().executeQuery(validationQuery1);
        assertTrue(result1.next());

        String validationQuery2 = "SELECT * FROM TABLES WHERE table_schema='" + name + "' AND table_name='test_table_2'";
        ResultSet result2 = context.createStatement().executeQuery(validationQuery2);
        assertTrue(result2.next());
    }

    @Test
    public void insertsRowIntoTable() throws SQLException {

        context.execute("CREATE TABLE test_table (column_name INT)");
        context.execute("INSERT INTO test_table VALUES (1)");


        ResultSet result = context.createStatement().executeQuery("SELECT * FROM test_table");
        assertTrue(result.next());
    }

    @Test
    public void insertsFewRowsIntoTwoTables() throws FileNotFoundException, SQLException {

        context.execute(getFile("/test_create_tables.sql"));
        context.execute(getFile("/test_inserts.sql"));

        ResultSet result1 = context.createStatement().executeQuery("SELECT * FROM test_table_1");
        assertTrue(result1.next());

        ResultSet result2 = context.createStatement().executeQuery("SELECT * FROM test_table_2");
        assertTrue(result2.next());
    }

    @Test
    public void insertsFewRowsIntoTablesFromMultipleFile() throws FileNotFoundException, SQLException {

        context.execute(getFile("/test_create_tables.sql"));
        context.execute(getFile("/test_create_table3.sql"));
        context.execute(getFile("/test_inserts.sql"));
        context.execute(getFile("/test_insert_table3.sql"));

        ResultSet result1 = context.createStatement().executeQuery("SELECT * FROM test_table_1");
        assertTrue(result1.next());

        ResultSet result2 = context.createStatement().executeQuery("SELECT * FROM test_table_2");
        assertTrue(result2.next());

        ResultSet result3 = context.createStatement().executeQuery("SELECT * FROM test_table_3");
        assertTrue(result3.next());
    }

    @Test
    public void loadsDataFromCsvFile() throws SQLException {

        context.execute(getFile("/test_create_table3.sql"));
        context.loadCsv("test_table_3", getFile("/test_data_3.csv"));

        ResultSet result3 = context.createStatement().executeQuery("SELECT * FROM test_table_3");
        assertTrue(result3.next());
    }

    private File getFile(String fileName) {
        return new File(getClass().getResource(fileName).getPath());
    }

    private VerticaTestContext context;

    public class TestVerticaTestContext extends VerticaTestContext {

        @Override
        public String getApplicationName() {
            return "vertica_integration_test";
        }

        @Override
        public Credentials getDbAdminCredentials() {
            Credentials credentials = new Credentials();
            credentials.setHost("TODO: put here your vertica server");
            credentials.setPort("5433");
            credentials.setDatabase("TODO: ");
            credentials.setUsername("TODO: ");
            credentials.setPassword("TODO: ");

            return credentials;
        }

    }
}
