package kpp.jdts.csv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class CSVDriver implements java.sql.Driver
{
  
  static
  {
    try
    {
      DriverManager.registerDriver(new CSVDriver());
    }
    catch (SQLException e)
    {
      kpp.jtds.core.Logger.error(e.getMessage());
    }
  }
  
  final static String URL_PREFIX = "jdbc:csv:file://";
  
  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    url = url.replace(URL_PREFIX, "");
    
    int lastIndexOfSlash = url.lastIndexOf("/");
    String dir = url.substring(0, lastIndexOfSlash);
    String fileNamePattern = url.substring(lastIndexOfSlash + 1);
    
    CSVConnection ret = new CSVConnection();
    ret.setCatalog(dir);
    ret.setPattern(fileNamePattern);
    return ret;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException
  {
    return url != null && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
  {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion()
  {
    return 1;
  }

  @Override
  public int getMinorVersion()
  {
    return 0;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException
  {
    return kpp.jtds.core.Logger.log;
  }

}
