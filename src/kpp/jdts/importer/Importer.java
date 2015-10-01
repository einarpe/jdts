package kpp.jdts.importer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import kpp.jdts.csv.FileStringBuilder;
import kpp.jtds.core.Logger;
import kpp.jtds.core.Step;

public abstract class Importer
{
  
  
  public final static SimpleDateFormat ShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  
  public final static SimpleDateFormat LongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  public static final String CP_INTO = "into";

  public static final String CP_QUERY = "query";

  public static final String CP_TRUNCATE = "truncate";

  public static final String CP_BEHAVIOUR = "behaviour";
  
  protected Step step;
  
  protected StringBuilder columnsFromResultSet;
  
  protected FileStringBuilder fsb;
  
  protected Properties config = new Properties();

  /** If there should be last semicolon on each line of CSV? */
  protected boolean appendLastSemicolon = true;
  
  public Importer(Step step)
  {
    this.step = step;
  }
  
  public Step getStep()
  {
    return step;
  }
  
  /** 
   * Preparation of CSV file. Function reads data based on query and writes it to temporary CSV file.
   * @return rows read from source database
   */
  public int prepare() throws Exception
  {
    Logger.info("Preparing data. Starting querying table ", config.getProperty(CP_INTO), "... ");
    
    int rows = 0;
    try (PreparedStatement stmt = step.getDTS().getSourceConnection().prepareStatement(config.getProperty(CP_QUERY)))
    {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      
      columnsFromResultSet = new StringBuilder();
      for (int k = 1; k <= columnCount; k++)
      {
        columnsFromResultSet.append(rsmd.getColumnLabel(k));
        if (k < columnCount)
          columnsFromResultSet.append(',');
      }
      
      fsb = new FileStringBuilder();
      while (rs.next())
      {
        for (int k = 1; k <= columnCount; k++)
        {
          fsb.append(obj2str(rs.getObject(k), rsmd.getColumnTypeName(k)));
          if (k == columnCount)
          {
            if (appendLastSemicolon)
              fsb.append(";");
          }
          else
            fsb.append(";");
        }
        
        fsb.append("\r\n");
        rows++;
      }
      fsb.close();
    }
    
    Logger.info("Preparation data ended. Row count for query: " + rows);
    return rows;
  }

  public void setPropertiesFromXml(Element element)
  {
    config.setProperty(CP_INTO, element.getAttribute("into"));
    NodeList queries = element.getElementsByTagName("query");
    if (queries.getLength() > 0)
      config.setProperty(CP_QUERY, ((Element)queries.item(0)).getTextContent());
    
    config.setProperty(CP_TRUNCATE, element.getAttribute("truncate").equalsIgnoreCase("true") ? "true" : "false");
    config.setProperty(CP_BEHAVIOUR, element.getAttribute("behaviour"));
  }
  
  @Override
  public String toString()
  {
    return String.format("into=%s ", config.getProperty(CP_INTO));
  }
  
  /**
   * We have got not empty temporary file with CSV contents now. 
   * So perform merging it with destination table. 
   * @throws Exception
   */
  public void insert() throws Exception
  {
    String sqlQuery = "";
    if (Boolean.parseBoolean(config.getProperty(CP_TRUNCATE)))
    {
      Logger.info("Truncating... "); 
      sqlQuery = getTruncateQuery(); 
      Logger.debug(sqlQuery);
      
      step.destinationExecStatement(sqlQuery); 
      
      Logger.info("Truncating done.");
    }
    
    Logger.info("Inserting... ");
    sqlQuery = getLoadDataInfileQuery(); 
    Logger.debug(sqlQuery);
    
    step.destinationExecStatement(sqlQuery);
    
    Logger.info("Inserting done.");
  }

  /** Return TRUNCATE TABLE or DELETE FROM query. */
  protected String getTruncateQuery()
  {
    return String.format("Truncate Table %s", config.getProperty(CP_INTO));
  }
  
  /**
   * Convert given object to string readable by MYSQL LOAD DATA INFILE query.
   * @param object - object to convert
   * @param columnType - column type from ResultSetMetaData
   * @return string readable by Mysql CSV reader
   */
  protected Object obj2str(Object object, String columnType)
  {
    if (object == null)
      return "\\N"; // szpecjal for mysql
    
    if (object instanceof String)
      return object.toString().replace("\"", "\\\"").replace(";", "\\;");
    
    if (columnType.equalsIgnoreCase("date"))
      return ShortDateFormat.format((Date)object);
    
    if (columnType.equalsIgnoreCase("datetime"))
      return LongDateFormat.format((Date)object);
    
    return object;
  }
  
  protected abstract String getLoadDataInfileQuery();
}
