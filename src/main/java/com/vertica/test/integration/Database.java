package com.vertica.test.integration;

import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Takes care about setting up (creation of schema, user, tables, executing inserts) and cleaning up
 * (dropping schema and user).
 */
public class Database {

    private Schema schema;
    private Credentials credentials;

    private StatementExecutor dbAdminStatementExecutor;
    private StatementExecutor userStatementExecutor;

    public Database(Credentials credentials, String applicationName) {
        this.credentials = credentials;

        dbAdminStatementExecutor = new StatementExecutor(getDataSource());
        schema = new Schema(applicationName, dbAdminStatementExecutor);

        userStatementExecutor = new StatementExecutor(getUserDataSource());
    }

    public void setup() throws SQLException {
        schema.create();
        schema.createUserAndGrant();
    }

    public void cleanup() throws SQLException {
        schema.drop();
    }

    public DataSource getDataSource() {
        String database = credentials.getDatabase();
        String port = credentials.getPort();
        String host = credentials.getHost();
        String username = credentials.getUsername();
        String password = credentials.getPassword();

        BasicDataSource dataSource = getBasicDataSource(database, port, host, username, password);
        return dataSource;
    }

    public DataSource getUserDataSource() {
        String database = credentials.getDatabase();
        String port = credentials.getPort();
        String host = credentials.getHost();
        String username = schema.getUsername();
        String password = schema.getPassword();

        BasicDataSource dataSource = getBasicDataSource(database, port, host, username, password);
        return dataSource;
    }

    private BasicDataSource getBasicDataSource(String database, String port, String host, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.vertica.jdbc.Driver");
        dataSource.setUrl("jdbc:vertica://" + host + ":" + port + "/" + database);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    public Schema getSchema() {
        return schema;
    }

    public StatementExecutor getDbAdminStatementExecutor() {
        return dbAdminStatementExecutor;
    }

    public StatementExecutor getUserStatementExecutor() {
        return userStatementExecutor;
    }
}
