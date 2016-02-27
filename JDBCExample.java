import java.io.*;
import java.sql.*;
public class JDBCExample {
    public static void main(String[] args) throws Exception {
        // loading the PostgreSQL driver
        Class.forName("org.postgresql.Driver");
        // connection to the a database
        String host = args[0];
        String port = args[1];
        String databaseName = args[2];
        String username = args[3];
        String password = args[4];
        Connection connection = DriverManager.getConnection("jdbc:postgresql://"
                + host + ":" + port + "/" + databaseName, username, password);
        // setting auto commit to false
        connection.setAutoCommit(false);
        // (a)
        // execute a query
        Statement stmt = connection.createStatement();
        
        //System.out.println("Creating table emp...");
        String sql = "CREATE TABLE emp " +
                     "(eid NUMERIC(9, 0), " +
                     " ename VARCHAR(30), " + 
                     " age NUMERIC(3, 0), " + 
                     " salary NUMERIC(10, 2), " + 
                     " PRIMARY KEY ( eid ))";
        stmt.executeUpdate(sql);
        //System.out.println("Creating table dept...");
        sql = "CREATE TABLE dept " +
              "(did NUMERIC(2, 0), " +
              " dname VARCHAR(20), " + 
              " budget NUMERIC(10, 2), " +
              " managerid NUMERIC(9, 0), " +  
              " PRIMARY KEY ( did ), " + 
              " FOREIGN KEY ( managerid ) REFERENCES " + 
              " emp ( eid ))";
        stmt.executeUpdate(sql);
        //System.out.println("Creating table works...");
        sql = "CREATE TABLE works " +
              "(eid NUMERIC(9, 0), " +
              " did NUMERIC(9, 0), " + 
              " pct_time NUMERIC(3, 0), " +  
              " PRIMARY KEY ( eid, did ), " + 
              " FOREIGN KEY ( eid ) REFERENCES " + 
              " emp, " + 
              " FOREIGN KEY ( did ) REFERENCES " + 
              " dept)";
        stmt.executeUpdate(sql);
        //System.out.println("Created 3 tables...");
        stmt.close();
        // (b)
        File file = new File(args[5]);
        BufferedReader bR = null;
        String line = "";
        bR = new BufferedReader(new FileReader(file));
        
        //System.out.println("Insert records into table...");
        sql = "INSERT INTO emp " + 
              " (eid, ename, age, salary) VALUES " + 
              " (?,?,?,?)";
        PreparedStatement pstmt = connection.prepareStatement(sql);
        while ((line = bR.readLine())!=null)
        {
            String[] tuple = line.split(",");
            pstmt.setInt(1, Integer.parseInt(tuple[0]));
            pstmt.setString(2, tuple[1]);
            pstmt.setInt(3, Integer.parseInt(tuple[2]));
            pstmt.setInt(4, Integer.parseInt(tuple[3]));
            pstmt.executeUpdate();
        }
        pstmt.close();
        // (c)
        Statement sqlstmt = connection.createStatement();
        sql = "CREATE OR REPLACE FUNCTION getnames(minsalary real) " + 
              " RETURNS refcursor AS " + 
              "$BODY$" +
              "DECLARE mycurs refcursor; " +
              "BEGIN " +
              "OPEN mycurs FOR " +
              "SELECT DISTINCT ename " +
              "FROM            emp " +
              "WHERE           salary >= minsalary " +
              "ORDER BY        ename ASC; " +
              "RETURN mycurs; " +
              "END " +
              "$BODY$" +
              "LANGUAGE plpgsql;";
        sqlstmt.executeUpdate(sql);
        sqlstmt.close();
        // (d)
        int salary = Integer.parseInt(args[6]);
        //System.out.println("Creating statement to call func...");
        CallableStatement cstmt = connection.prepareCall("{? = call getnames(?)}");
        cstmt.registerOutParameter (1, Types.OTHER);
        cstmt.setInt(2, salary);
        cstmt.execute();
        ResultSet rs = (ResultSet) cstmt.getObject(1);
        ResultSetMetaData rsmd = rs.getMetaData();
        int col = rsmd.getColumnCount();
        while (rs.next())
        {
            for (int i=1; i <= col; i++)
            {
              String val = rs.getString(i);
              System.out.println(val);
            }
        }
        rs.close();
        cstmt.close();
        // closing database connection
        connection.close();
    }
}