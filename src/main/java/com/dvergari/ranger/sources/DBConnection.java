package com.dvergari.ranger.sources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

public class DBConnection {
    private final static int MAX_CONNECTIONS = 100;

    private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static String jdbc_db_url ;

    private static String jdbc_db_user ;
    private static String jdbc_db_pass ;

    private static GenericObjectPool gPool = null;

    public DBConnection(String url, String user, String password) {
        this.jdbc_db_url = url;
        this.jdbc_db_user = user;
        this.jdbc_db_pass = password;
    }

    public DataSource setUpPool() throws Exception {
        Class.forName(JDBC_DRIVER);

        gPool = new GenericObjectPool();
        gPool.setMaxActive(MAX_CONNECTIONS);

        ConnectionFactory cf = new DriverManagerConnectionFactory(jdbc_db_url, jdbc_db_user, jdbc_db_pass);

        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);

        return new PoolingDataSource(gPool);
    }

    public GenericObjectPool getConnectionPool() {
        return gPool;
    }

    public String getConnectionStatus() {
        return "Max.: " + getConnectionPool().getMaxActive() + "; Active: " + getConnectionPool().getNumActive() + "; Idle: " + getConnectionPool().getNumIdle();
    }
}
