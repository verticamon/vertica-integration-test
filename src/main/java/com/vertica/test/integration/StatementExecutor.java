package com.vertica.test.integration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class StatementExecutor {

    private DataSource dataSource;

    public StatementExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void execute(String statement) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.createStatement().execute(statement);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
