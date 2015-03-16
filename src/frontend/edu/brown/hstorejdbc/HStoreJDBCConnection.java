package edu.brown.hstorejdbc;

import java.lang.*;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseURL;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.jdbc.JDBCCallableStatement;
import org.hsqldb.jdbc.JDBCClob;
import org.hsqldb.jdbc.JDBCDatabaseMetaData;
import org.hsqldb.jdbc.JDBCNClob;
import org.hsqldb.jdbc.JDBCPreparedStatement;
import org.hsqldb.jdbc.JDBCResultSet;
import org.hsqldb.jdbc.JDBCSQLXML;
import org.hsqldb.jdbc.JDBCSavepoint;
import org.hsqldb.jdbc.JDBCStatement;
import org.hsqldb.jdbc.Util;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.utils.CollectionUtil;

public class HStoreJDBCConnection implements Connection {

    
    public synchronized Statement createStatement() throws SQLException {

        return null;
    }
    public synchronized boolean isClosed() throws SQLException {
        return isClosed;
    }
    public synchronized PreparedStatement prepareStatement(
            String sql) throws SQLException {
        return null;
    }
    public synchronized String getCatalog() throws SQLException {
        throw new UnsupportedOperationException();
        
    }
    public synchronized CallableStatement prepareCall(
            String sql) throws SQLException {
        return null;
    }
    public synchronized String nativeSQL(
            final String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }
    public synchronized void setReadOnly(
            boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
        
    }   
    public synchronized boolean getAutoCommit() throws SQLException {

        throw new UnsupportedOperationException();
    }
    
    public HStoreJDBCConnection(HsqlProperties props) throws SQLException {

        String user     = props.getProperty("user");
        String password = props.getProperty("password");
        
        String host     = props.getProperty("host");
        int    port     = props.getIntegerProperty("port", HStoreConstants.DEFAULT_PORT);   

        
        //String path     = props.getProperty("path");
        //String database = props.getProperty("database");     
     
//        if(host != null) {
//        }
//        // Connect to random host and using a random port that it's listening on
//        else if (this.catalog != null) {
//            Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
//            hostname = catalog_site.getHost().getIpaddr();
//            port = catalog_site.getProc_port();
//        }
        
        assert(host != null);
        assert(port > 0);       

        
        Client client = ClientFactory.createClient(128, null, false, null);
        try {
            client.createConnection(null, host, port, "user", "password");
            System.out.printf("connected to HStoreSite at %s:%d", host, port);
        } catch (Exception ex) {
            String msg = String.format("Failed to connect to HStoreSite at %s:%d", host, port);
            throw new RuntimeException(msg);
        }
    }    
    
    public synchronized void commit() throws SQLException {

        throw new UnsupportedOperationException();
    }
    public synchronized void setAutoCommit(
            boolean autoCommit) throws SQLException {

        throw new UnsupportedOperationException();
    }   
    public synchronized void rollback() throws SQLException {

        throw new UnsupportedOperationException();
    }
    public synchronized void rollback(
            Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public synchronized void close() throws SQLException {

        throw new UnsupportedOperationException();
    }
    
    
    public synchronized DatabaseMetaData getMetaData() throws SQLException {

        throw new UnsupportedOperationException();
    }
    
    
    public synchronized void setCatalog(String catalog) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public synchronized void setTransactionIsolation(
            int level) throws SQLException {

    }

    public synchronized int getTransactionIsolation() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public synchronized SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
        
    }

    public synchronized void clearWarnings() throws SQLException {

        throw new UnsupportedOperationException();
    }

  
    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException {

        throw new UnsupportedOperationException();
    }

   
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {

        throw new UnsupportedOperationException();
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        throw new UnsupportedOperationException();
    }
    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public synchronized java.util
            .Map<java.lang.String,
                 java.lang.Class<?>> getTypeMap() throws SQLException {

        

        return new java.util.HashMap<java.lang.String, java.lang.Class<?>>();
    }

    public synchronized void setTypeMap(Map<String,
            Class<?>> map) throws SQLException {

        

        throw new UnsupportedOperationException();
    }

    public synchronized void setHoldability(
            int holdability) throws SQLException {
        throw new UnsupportedOperationException();
      
    }

    public synchronized int getHoldability() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public synchronized Savepoint setSavepoint(String name) throws SQLException {

        throw new UnsupportedOperationException();
      
    }

  
    public synchronized Savepoint setSavepoint() throws SQLException {

        throw new UnsupportedOperationException();
      
    }

    public synchronized void releaseSavepoint(
            Savepoint savepoint) throws SQLException {

        throw new UnsupportedOperationException();
    }

    

    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public synchronized PreparedStatement prepareStatement(String sql,
            int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
        
    }

    public synchronized PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public synchronized PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Clob createClob() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
       
    }

    public NClob createNClob() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public SQLXML createSQLXML() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public boolean isValid(int timeout) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public void setClientInfo(String name,
                              String value) throws SQLClientInfoException {

        throw new UnsupportedOperationException();
    }

    public void setClientInfo(
            Properties properties) throws SQLClientInfoException {

        throw new UnsupportedOperationException();
    }

    public String getClientInfo(String name) throws SQLException {

   
        throw new UnsupportedOperationException();
       
    }

    public Properties getClientInfo() throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Array createArrayOf(String typeName,
                               Object[] elements) throws SQLException {

        throw new UnsupportedOperationException();
    }

    public Struct createStruct(String typeName,
                               Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        checkClosed();

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }

    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {

        checkClosed();

        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//---------------------- internal implementation ---------------------------
// -------------------------- Common Attributes ------------------------------

    /** Initial holdability */
    int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;

    /**
     * Properties for the connection
     *
     */
    HsqlProperties connProperties;

    /**
     * This connection's interface to the corresponding Session
     * object in the database engine.
     */
    SessionInterface sessionProxy;

    /**
     * Is this an internal connection?
     */
    boolean isInternal;

    /** Is this connection to a network server instance. */
    protected boolean isNetConn;

    /**
     * Is this connection closed?
     */
    boolean isClosed;

    /** The first warning in the chain. Null if there are no warnings. */
    private SQLWarning rootWarning;

    /** Synchronizes concurrent modification of the warning chain */
    private Object rootWarning_mutex = new Object();

    /** ID sequence for unnamed savepoints */
    private int savepointIDSequence;

    /**
     * Constructs a new external <code>Connection</code> to an HSQLDB
     * <code>Database</code>. <p>
     *
     * This constructor is called on behalf of the
     * <code>java.sql.DriverManager</code> when getting a
     * <code>Connection</code> for use in normal (external)
     * client code. <p>
     *
     * Internal client code, that being code located in HSQLDB SQL
     * functions and stored procedures, receives an INTERNAL
     * connection constructed by the {@link
     * #JDBCConnection(org.hsqldb.SessionInterface)
     * JDBCConnection(SessionInterface)} constructor. <p>
     *
     * @param props A <code>Properties</code> object containing the connection
     *      properties
     * @exception SQLException when the user/password combination is
     *     invalid, the connection url is invalid, or the
     *     <code>Database</code> is unavailable. <p>
     *
     *     The <code>Database</code> may be unavailable for a number
     *     of reasons, including network problems or the fact that it
     *     may already be in use by another process.
     */
    

    /**
     *  The default implementation simply attempts to silently {@link
     *  #close() close()} this <code>Connection</code>
     */
    protected void finalize() {

        try {
            close();
        } catch (SQLException e) {
        }
    }

    synchronized int getSavepointID() {
        return savepointIDSequence++;
    }

    /**
     * Retrieves this connection's JDBC url.
     *
     * This method is in support of the JDBCDatabaseMetaData.getURL() method.
     * @return the database connection url with which this object was
     *      constructed
     * @throws SQLException if this connection is closed
     */
    synchronized String getURL() throws SQLException {

        checkClosed();

        return isInternal ? sessionProxy.getInternalConnectionURL()
                          : connProperties.getProperty("url");
    }

    /**
     * An internal check for closed connections. <p>
     *
     * @throws SQLException when the connection is closed
     */
    synchronized void checkClosed() throws SQLException {

        if (isClosed) {
            throw Util.connectionClosedException();
        }
    }

    /**
     * Adds another SQLWarning to this Connection object's warning chain.
     *
     * @param w the SQLWarning to add to the chain
     */
    void addWarning(SQLWarning w) {

        // PRE:  w is never null
        synchronized (rootWarning_mutex) {
            if (rootWarning == null) {
                rootWarning = w;
            } else {
                rootWarning.setNextWarning(w);
            }
        }
    }

    /**
     * Clears the warning chain without checking if this Connection is closed.
     */
    void clearWarningsNoCheck() {

        synchronized (rootWarning_mutex) {
            rootWarning = null;
        }
    }

    /**
     * Translates <code>ResultSet</code> type, adding to the warning
     * chain if the requested type is downgraded. <p>
     *
     * Up to and including HSQLDB 1.7.2,  <code>TYPE_FORWARD_ONLY</code> and
     * <code>TYPE_SCROLL_INSENSITIVE</code> are passed through. <p>
     *
     * Starting with 1.7.2, while <code>TYPE_SCROLL_SENSITIVE</code> is
     * downgraded to <code>TYPE_SCROLL_INSENSITIVE</code> and an SQLWarning is
     * issued. <p>
     *
     * @param type of <code>ResultSet</code>; one of
     *     <code>JDBCResultSet.TYPE_XXX</code>
     * @return the actual type that will be used
     * @throws SQLException if type is not one of the defined values
     */
    int xlateRSType(int type) throws SQLException {

        SQLWarning w;
        String     msg;

        switch (type) {

            case JDBCResultSet.TYPE_FORWARD_ONLY :
            case JDBCResultSet.TYPE_SCROLL_INSENSITIVE : {
                return type;
            }
            case JDBCResultSet.TYPE_SCROLL_SENSITIVE : {
                msg = "TYPE_SCROLL_SENSITIVE => TYPE_SCROLL_SENSITIVE";
                w = new SQLWarning(msg, "SOO10",
                                   ErrorCode.JDBC_INVALID_ARGUMENT);

                addWarning(w);

                return JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
            }
            default : {
                msg = "ResultSet type: " + type;

                throw Util.invalidArgument(msg);
            }
        }
    }

    /**
     * Translates <code>ResultSet</code> concurrency, adding to the warning
     * chain if the requested concurrency is downgraded. <p>
     *
     * Starting with HSQLDB 1.7.2, <code>CONCUR_READ_ONLY</code> is
     * passed through while <code>CONCUR_UPDATABLE</code> is downgraded
     * to <code>CONCUR_READ_ONLY</code> and an SQLWarning is issued.
     *
     * @param concurrency of <code>ResultSet</code>; one of
     *     <code>JDBCResultSet.CONCUR_XXX</code>
     * @return the actual concurrency that will be used
     * @throws SQLException if concurrency is not one of the defined values
     */
    int xlateRSConcurrency(int concurrency) throws SQLException {

        SQLWarning w;
        String     msg;

        switch (concurrency) {

            case JDBCResultSet.CONCUR_READ_ONLY : {
                return concurrency;
            }
            case JDBCResultSet.CONCUR_UPDATABLE : {
                msg = "CONCUR_UPDATABLE => CONCUR_READ_ONLY";
                w = new SQLWarning(msg, "SOO10",
                                   ErrorCode.JDBC_INVALID_ARGUMENT);

                addWarning(w);

                return JDBCResultSet.CONCUR_READ_ONLY;
            }
            default : {
                msg = "ResultSet concurrency: " + concurrency;

                throw Util.invalidArgument(msg);
            }
        }
    }

    /**
     * Resets this connection so it can be used again. Used when connections are
     * returned to a connection pool.
     *
     * @throws SQLException if a database access error occurs
     */
    public void reset() throws SQLException {

        try {
            this.sessionProxy.resetSession();
        } catch (HsqlException e) {
            throw Util.sqlException(ErrorCode.X_08006, e.getMessage(), e);
        }
    }

    /**
     * is called from within nativeSQL when the start of an JDBC escape sequence is encountered
     */
    private int onStartEscapeSequence(String sql, StringBuffer sb,
                                      int i) throws SQLException {

        sb.setCharAt(i++, ' ');

        i = StringUtil.skipSpaces(sql, i);

        if (sql.regionMatches(true, i, "fn ", 0, 3)
                || sql.regionMatches(true, i, "oj ", 0, 3)
                || sql.regionMatches(true, i, "ts ", 0, 3)) {
            sb.setCharAt(i++, ' ');
            sb.setCharAt(i++, ' ');
        } else if (sql.regionMatches(true, i, "d ", 0, 2)
                   || sql.regionMatches(true, i, "t ", 0, 2)) {
            sb.setCharAt(i++, ' ');
        } else if (sql.regionMatches(true, i, "call ", 0, 5)) {
            i += 4;
        } else if (sql.regionMatches(true, i, "?= call ", 0, 8)) {
            sb.setCharAt(i++, ' ');
            sb.setCharAt(i++, ' ');

            i += 5;
        } else if (sql.regionMatches(true, i, "escape ", 0, 7)) {
            i += 6;
        } else {
            i--;

            throw Util.sqlException(
                Error.error(
                    ErrorCode.JDBC_CONNECTION_NATIVE_SQL, sql.substring(i)));
        }

        return i;
    }

    public void setSchema(String schema) throws SQLException {
        throw new SQLException();
    }

    public String getSchema() throws SQLException {
        throw new SQLException();
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLException();
    }
    
    public synchronized boolean isReadOnly() throws SQLException {

        throw new SQLException();
    }
}
