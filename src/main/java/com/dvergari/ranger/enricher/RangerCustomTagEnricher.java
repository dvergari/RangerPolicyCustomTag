package com.dvergari.ranger.enricher;

import org.apache.ranger.plugin.contextenricher.RangerAbstractContextEnricher;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dvergari.ranger.sources.DBConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RangerCustomTagEnricher extends RangerAbstractContextEnricher {
    private static final Logger LOG = LoggerFactory.getLogger(RangerCustomTagEnricher.class);

    Map<String, String[]> cacheUsersTag = null;

    private String table = null;

    private String contextName = "USERTAG";

    private DBConnection dbConn ;
    private DataSource dataSource;

    private long _tagRefresh = System.currentTimeMillis();

    private static final long TAG_REFRESH_TIMEOUT = 60000L;

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerTagEnricher.init(" + enricherDef + ")");
        }

        super.init();

        contextName = getOption("contextName", "USERTAG");

        String dbUrl = getOption("dbUrl");
        String username = getOption("username");
        String password = getOption("password");

        table = getOption("table");
        try {
            setupConnection(dbUrl, username, password);
        } catch (Exception ex) {
            LOG.error("Can't connect to database. Using cached information (if any)");
            LOG.error(ex.toString());
        }

        cacheUsersTag = getCacheUsersTag(table);

    }

    private void setupConnection(String dbUrl, String username, String password) throws Exception {
        dbConn = new DBConnection(dbUrl, username, password) ;
        dataSource = dbConn.setUpPool();
        if (LOG.isDebugEnabled()) {
            LOG.debug(dbConn.getConnectionStatus());
        }
    }

    private Map<String, String[]> getCacheUsersTag(String table) {
        if (dbConn != null) {
            Map<String, String[]> cache = new HashMap<>();
            if (LOG.isDebugEnabled()) {
                LOG.debug(dbConn.getConnectionStatus());
            }
            ResultSet rsObj = null;
            Connection connObj = null;
            PreparedStatement pstmtObj = null;
            try {
                connObj = dataSource.getConnection();
                pstmtObj = connObj.prepareStatement("SELECT user, taglist FROM " + table);
                rsObj = pstmtObj.executeQuery();
                while (rsObj.next()) {
                    String user = rsObj.getString("user");
                    String[] tags = null;
                    try {
                       tags = rsObj.getString("taglist").split(",");
                    } catch (NullPointerException ex) {
                       if (LOG.isDebugEnabled()) {
                           LOG.debug("Tag list is empty. Returning null value");
                       }
                    }
                    cache.put(user, tags);
                }
                rsObj.close();
                connObj.close();
            } catch (SQLException exception) {
                LOG.error("Can't query the table. Returning old cache");
                LOG.error(exception.getMessage());
                return cacheUsersTag;
            }
            _tagRefresh = System.currentTimeMillis();
            return cache;
        } else {
            LOG.warn("Can't reload datasource. Connection was not established");
            return null;

        }
    }

    @Override
    public void enrich(RangerAccessRequest rangerAccessRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerTagEnricher.enrich(" + rangerAccessRequest +")");
        }
        if (System.currentTimeMillis() - TAG_REFRESH_TIMEOUT > _tagRefresh) { //It's time to refresh cache
            cacheUsersTag = getCacheUsersTag(table);
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> RangerTagEnricher.enrich: Refreshing cache");
            }
        }
        try {
            if (rangerAccessRequest != null && cacheUsersTag != null) {
                Map<String, Object> context = rangerAccessRequest.getContext();
                if (rangerAccessRequest.getUser() != null) {
                    String[] tags = cacheUsersTag.get(rangerAccessRequest.getUser());
                    if (context != null && tags != null) {
                        rangerAccessRequest.getContext().put(contextName, tags);
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("==> RangerTagEnricher.enrich(): skipping due to missing user");
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOG.warn("Can't find user " + rangerAccessRequest.getUser() + " in table");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerTagEnricher.enrich(" + rangerAccessRequest +")");
        }
    }


}
