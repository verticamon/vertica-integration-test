package com.vertica.test.integration;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 * Setups and cleans up the database schema for a test.
 */
public abstract class VerticaTestContext {

    private Database database = new Database(getDbAdminCredentials(), getApplicationName());

    public abstract String getApplicationName();

    public abstract Credentials getDbAdminCredentials();

    public VerticaTestContext setup() {

        try {
            getDatabase().setup();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    public VerticaTestContext cleanup() {

        try {
            getDatabase().cleanup();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    public DataSource getUserDataSource() {
        return getDatabase().getUserDataSource();
    }

    public DataSource getDbAdminDataSource() {
        return getDatabase().getDbAdminStatementExecutor().getDataSource();
    }

    public VerticaTestContext execute(String statement) throws SQLException {
        getDatabase().getUserStatementExecutor().execute(statement);

        return this;
    }

    public VerticaTestContext execute(File inserts) {

        try {
            Scanner scanner = getScanner(inserts);

            while (scanner.hasNext()) {
                String insert = scanner.next();
                execute(insert);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    public void loadCsv(String tableName, File file) {

        try {
            Statement statement = createStatement();

            String schemaName = getDatabase().getSchema().getSchemaName();
            String path = file.getPath();
            String query = "COPY " + schemaName + "." + tableName + " FROM LOCAL '" + path + "' WITH DELIMITER ',' ENCLOSED BY '" + '"' + "' ABORT ON ERROR";

            statement.execute(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Scanner getScanner(File tables) throws FileNotFoundException {
        return new Scanner(tables).useDelimiter(";");
    }

    public Database getDatabase() {
        return database;
    }

    public Statement createStatement() {
        try {
            return getUserDataSource().getConnection().createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
