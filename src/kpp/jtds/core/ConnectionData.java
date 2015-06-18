package kpp.jtds.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.w3c.dom.Element;

public class ConnectionData
{
  private String url;
  private String user;
  private String password;
  
  private Connection connection = null;
  
  public Connection getConnection() throws SQLException
  {
    if (connection != null && !connection.isClosed())
      return connection;
    
    connection = DriverManager.getConnection(url, user, password);
    return connection;
  }
  
  public static void test() throws Exception
  {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    
    String url = "jdbc:sqlserver://localhost:19570;databaseName=master;integratedSecurity=true;";
    
    Connection connection = DriverManager.getConnection(url, "", "");
    System.out.println(connection);
    
    PreparedStatement ps = connection.prepareStatement("Select SYSTEM_USER ");
    ResultSet rs = ps.executeQuery();
    if (rs.next())
      System.out.println("current_user = " + rs.getObject(1));
    rs.close();
    ps.close();
  }

  public static ConnectionData fromXml(Element el) throws ClassNotFoundException
  {
    ConnectionData result = new ConnectionData();
    result.url = el.getAttribute("url");
    result.user = el.getAttribute("user"); 
    result.password = el.getAttribute("password");
    
    Class.forName(el.getAttribute("driver"));
    
    return result;
  }
}
