package edu.brown.hstorejdbc;
import java.sql.*;
import java.util.Properties;

public class JDBCClientExample {

    /**
     * @param args
     */
    public static void main(String[] args) {
        
        Connection conn;
        String query = "select top 10 * from acccess_info;"; // access_info is a table in benchmark tm1
        
        try {
            //Class.forName("edu.brown.hstorejdbc" );
            //jdbc:postgresql://host:port/database
            String url = "jdbc:hstorejdbc://localhost/tm1";     
            Properties props = new Properties();
            props.setProperty("user","user");
            props.setProperty("password","password");
            props.setProperty("host","localhost");
            props.setProperty("port","21212");
            props.setProperty("benchmark","tm1");  // not used
            //conn = DriverManager.getConnection(url, props);
            conn = HStoreJDBCDriver.getConnection(url, props);
            
        } catch (Exception e) {
            e.printStackTrace();            
        }
    }

}
