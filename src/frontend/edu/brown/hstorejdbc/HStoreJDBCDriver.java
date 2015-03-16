package edu.brown.hstorejdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.hsqldb.ErrorCode;

import org.hsqldb.jdbc.Util;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;

public class HStoreJDBCDriver implements Driver {

    /**
     * Default constructor
     */
    public HStoreJDBCDriver() {
    }

    public Connection connect(String url,
                              Properties info) throws SQLException {
        return getConnection(url, info);
    }

    public static Connection getConnection(String url,
            Properties info) throws SQLException {

        final HsqlProperties props = DatabaseURL.parseURL(url, true, false);

        if (props == null) {

            // supposed to be an HSQLDB driver url but has errors
            throw Util.invalidArgument();
        } else if (props.isEmpty()) {

            // is not an HSQLDB driver url
            return null;
        }
        props.addProperties(info);

        long timeout = DriverManager.getLoginTimeout();

        
        if (timeout == 0) {

            // no timeout restriction
            return new HStoreJDBCConnection(props);
        } else {
            // don't support timeout yet
        }

        throw Util.sqlException(ErrorCode.X_08501);
    }

    /**
     *  Returns true if the driver thinks that it can open a connection to
     *  the given URL. Typically drivers will return true if they understand
     *  the subprotocol specified in the URL and false if they don't.
     *
     * @param  url the URL of the database
     * @return  true if this driver can connect to the given URL
     */

    // fredt@users - patch 1.7.0 - allow mixedcase url's
    public boolean acceptsURL(String url) {

        if (url == null) {
            return false;
        }

        return url.regionMatches(true, 0, DatabaseURL.S_URL_PREFIX, 0,
                                 DatabaseURL.S_URL_PREFIX.length());
    }

    /**
     *  Gets information about the possible properties for this driver. <p>
     *
     *  The getPropertyInfo method is intended to allow a generic GUI tool
     *  to discover what properties it should prompt a human for in order to
     *  get enough information to connect to a database. Note that depending
     *  on the values the human has supplied so far, additional values may
     *  become necessary, so it may be necessary to iterate though several
     *  calls to getPropertyInfo.<p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB uses the values submitted in info to set the value for
     * each DriverPropertyInfo object returned. It does not use the default
     * value that it would use for the property if the value is null. <p>
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @param  url the URL of the database to which to connect
     * @param  info a proposed list of tag/value pairs that will be sent on
     *      connect open
     * @return  an array of DriverPropertyInfo objects describing possible
     *      properties. This array may be an empty array if no properties
     *      are required.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {

        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        String[]             choices = new String[] {
            "true", "false"
        };
        DriverPropertyInfo[] pinfo   = new DriverPropertyInfo[6];
        DriverPropertyInfo   p;

        if (info == null) {
            info = new Properties();
        }
        p          = new DriverPropertyInfo("user", null);
        p.value    = info.getProperty("user");
        p.required = true;
        pinfo[0]   = p;
        p          = new DriverPropertyInfo("password", null);
        p.value    = info.getProperty("password");
        p.required = true;
        pinfo[1]   = p;
        p          = new DriverPropertyInfo("get_column_name", null);
        p.value    = info.getProperty("get_column_name", "true");
        p.required = false;
        p.choices  = choices;
        pinfo[2]   = p;
        p          = new DriverPropertyInfo("ifexists", null);
        p.value    = info.getProperty("ifexists", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[3]   = p;
        p          = new DriverPropertyInfo("default_schema", null);
        p.value    = info.getProperty("default_schema", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[4]   = p;
        p          = new DriverPropertyInfo("shutdown", null);
        p.value    = info.getProperty("shutdown", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[5]   = p;

        return pinfo;
    }

    /**
     *  Gets the driver's major version number.
     *
     * @return  this driver's major version number
     */
    public int getMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     *  Gets the driver's minor version number.
     *
     * @return  this driver's minor version number
     */
    public int getMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    /**
     * Reports whether this driver is a genuine JDBC Compliant<sup><font
     * size=-2>TM</font></sup> driver. A driver may only report
     * <code>true</code> here if it passes the JDBC compliance tests; otherwise
     * it is required to return <code>false</code>. <p>
     *
     * JDBC compliance requires full support for the JDBC API and full support
     * for SQL 92 Entry Level. <p>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     *  HSQLDB 1.9.0 is aimed to be compliant with JDBC 4.0 specification.
     *  It supports SQL 92 Entry Level and beyond.
     * </div> <!-- end release-specific documentation -->
     *
     * This method is not intended to encourage the development of non-JDBC
     * compliant drivers, but is a recognition of the fact that some vendors
     * are interested in using the JDBC API and framework for lightweight
     * databases that do not support full database functionality, or for
     * special databases such as document information retrieval where a SQL
     * implementation may not be feasible.
     *
     * @return <code>true</code> if this driver is JDBC Compliant;
     *         <code>false</code> otherwise
     */
    public boolean jdbcCompliant() {
        return true;
    }

    static {
        try {
            DriverManager.registerDriver(new HStoreJDBCDriver());
        } catch (Exception e) {
        }
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
