package com.vertica.test.integration;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Represents schema for a test. Its responsible for creation of schema and user that has set a schema as search path.
 */
public class Schema {

    private static final String PREFIX = "_test_";

    private StatementExecutor statementExecutor;

    private String schemaName;

    public Schema(String appName, StatementExecutor statementExecutor) {
        String name = PREFIX + appName + "_" + UUID.randomUUID().toString().replace('-', '_');
        this.schemaName = name;

        this.statementExecutor = statementExecutor;
    }

    public void create() throws SQLException {
        String createSchema = "CREATE SCHEMA " + schemaName;
        statementExecutor.execute(createSchema);
    }

    public void createUserAndGrant() throws SQLException {
        String username = getUsername();
        String password = getPassword();
        statementExecutor.execute("CREATE USER " + username + " IDENTIFIED BY '" + password + "' ");
        statementExecutor.execute("GRANT USAGE ON SCHEMA " + schemaName + " TO " + username);
        statementExecutor.execute("ALTER USER " + username + " WITH SEARCH_PATH " + schemaName);
        statementExecutor.execute("GRANT ALL ON SCHEMA " + schemaName + " TO " + username);
        statementExecutor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA " + schemaName + " TO " + username);
    }

    public void drop() throws SQLException {
        statementExecutor.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");

        String username = getUsername();
        statementExecutor.execute("DROP USER IF EXISTS " + username + " CASCADE");
    }

    public String getPassword() {
        return "0123456789";
    }

    public String getUsername() {
        return schemaName + "_user";
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
