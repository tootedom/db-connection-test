package org.greencheek.db;

import org.apache.tomcat.jdbc.pool.PoolProperties;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  Quick class that keeps a db connection open, and on a kill -s SIGUSR2 <pid>
 *  will connect to the db (obtain a connection from the connection pool), create
 *  a statement and execute it.
 *
 *  The db connection is established at the beginning of the main.
 *  Class is kept running by a simple non deamon thread
 *
 *  Purpose of the class is to just see what happens when a connection
 *  is timed out (i.e. is the firewall dropping the connection, or is a RST sent back)
 */
public class LongRunningDBConnection {

    private static javax.sql.DataSource dataSource;
    private static volatile Thread deamon;


    public static void main(String[] args) throws Exception {


        PoolProperties p = new PoolProperties();
        p.setUrl(System.getProperty("db.url","jdbc:mysql://localhost:3306/mysql"));
        p.setDriverClassName(System.getProperty("db.driver","com.mysql.jdbc.Driver"));
        p.setUsername(System.getProperty("db.user","root"));
        p.setPassword(System.getProperty("db.password",""));
        p.setTestOnBorrow(true);
        p.setValidationQuery("/* ping */ SELECT 1");
        p.setValidationInterval(Integer.getInteger("db.validationTimeout",30000));

        p.setMaxActive(Integer.getInteger("db.maxConnActive",1));
        p.setInitialSize(1);
        p.setMaxWait(10000);

        p.setMinIdle(1);
        p.setLogAbandoned(true);
        p.setJdbcInterceptors(
                "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"+
                        "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");

        org.apache.tomcat.jdbc.pool.DataSource tcDs = new org.apache.tomcat.jdbc.pool.DataSource(p);
        tcDs.createPool();
        dataSource = tcDs;

        deamon = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date()) + ": sleeping");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        break;
                    }
                }
            }
        });
        deamon.setDaemon(false);
        deamon.start();

        Signal.handle(new Signal("INT"), new SignalHandler() {
            public void handle(Signal sig) {
                // increment the signal counter
                System.out.println("=============");
                System.out.println("Signal INT caught:" + sig.getName());
                System.out.println("=============");
                System.out.flush();

                deamon.interrupt();
            }

        });

        Signal.handle(new Signal("USR2"), new SignalHandler() {
            public void handle(Signal sig) {
                // increment the signal counter
                System.out.println("=============");
                System.out.println("Signal USR2 caught:" + sig.getName());
                System.out.println("=============");
                System.out.flush();

                doQuery();
            }

        });

        doQuery();


    }

    private static void doQuery() {
        System.out.println("=============");
        System.out.println("Performing Query : " + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date()));
        System.out.println("=============");
        System.out.flush();
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;

        try {
            System.out.println("Obtaining Connection");
            System.out.flush();
            con = dataSource.getConnection();
            System.out.println("Creating Statement");
            System.out.flush();
            st = con.createStatement();
            System.out.println("Executing Query");
            System.out.flush();
            rs = st.executeQuery("select * from user");
            int cnt = 1;
            while (rs.next()) {
                System.out.println((cnt++)+". Host:" +rs.getString("Host")+
                        " User:"+rs.getString("User"));
                System.out.flush();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(rs!=null) {
                try {
                    rs.close();
                } catch(SQLException e) {
                    System.err.println("exception closing results set.");
                    e.printStackTrace();
                }
            }
            if(st!=null) {
                try {
                    st.close();
                }catch (SQLException e) {
                    System.err.println("exception closing statement.");
                    e.printStackTrace();
                }
            }
            if (con!=null) try {con.close();}catch (Exception ignore) {}
        }
    }
}
