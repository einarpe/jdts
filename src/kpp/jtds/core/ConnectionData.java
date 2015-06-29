package kpp.jtds.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.w3c.dom.Element;

/** 
 * Class for storing connection data such as: url to database, user and password used to login and driver.
 */
public class ConnectionData
{
  private String url;
  private String user;
  private String password;
  
  private Connection connection = null;
  
  /** Forbidden constructor */
  private ConnectionData() { }
  
  /**
   * Returns opened connection to database.
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException
  {
    if (connection != null && !connection.isClosed())
      return connection;
    
    connection = DriverManager.getConnection(url, user, password);
    return connection;
  }
  
  /**
   * Create instance of ConnectionData class based on XML element.
   * @param el - xml element
   * @return
   * @throws ClassNotFoundException when driver class will not be found in classpath
   */
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
