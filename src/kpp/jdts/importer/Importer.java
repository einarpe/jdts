package kpp.jdts.importer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashSet;
import java.util.Properties;

import kpp.jdts.csv.FileStringBuilder;
import kpp.jdts.csv.dialect.Dialect;
import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.GlobalConfiguration;
import kpp.jtds.core.ExecuteStep;
import kpp.jtds.core.Logger;
import kpp.jtds.core.Step;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public abstract class Importer
{
  public static final String CP_INTO = "into";

  public static final String CP_QUERY = "query";

  public static final String CP_TRUNCATE = "truncate";

  public static final String CP_BEHAVIOUR = "behaviour";
  
  protected Step step;
  
  protected StringBuilder columnsFromResultSet;
  
  protected FileStringBuilder fsb;
  
  /** Some properties of importer. */
  protected Properties config = new Properties();

  /** Dialect to use when saving data to CSV file. */
  protected static Dialect dialect = Dialects.Default;
  
  /** List of nasty column names which should be quoted. */
  protected static HashSet<String> quotedColumns = new HashSet<String>();
  
  /** Character using for quoting nasty columns, so RMDBs will not complain about it. */
  protected static String quoteColumnChar = "";

  /** If there should be last semicolon on each line of CSV? */
  protected static boolean appendLastSemicolon = true;
  
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
    Logger.info("Preparing data for table ", config.getProperty(CP_INTO), "... ");
    Dialect dlct = getDialect();
    
    int rows = 0;
    try (PreparedStatement stmt = step.getDTS().getSourceConnection().prepareStatement(config.getProperty(CP_QUERY)))
    {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();
      
      columnsFromResultSet = new StringBuilder();
      for (int k = 1; k <= columnCount; k++)
      {
        String columnName = quote(rsmd.getColumnLabel(k));
        columnsFromResultSet.append(columnName);
        if (k < columnCount)
          columnsFromResultSet.append(',');
      }
      
      fsb = new FileStringBuilder();
      while (rs.next())
      {
        for (int k = 1; k <= columnCount; k++)
        {
          fsb.append(dlct.objectToString(rs.getObject(k), rsmd.getColumnTypeName(k)));
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
  
  /** Copy some useful properties from attributes/children/etc. of step XML element. */ 
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
  
  protected String quote(String columnName)
  {
    if (quotedColumns.contains(columnName.toLowerCase()))
      return String.format("%s%s%s", quoteColumnChar, columnName, quoteColumnChar);
    
    return columnName;
  }
  
  /** Execute one or more execute steps of given name ... */
  public void executeStepExecute(String[] execNames) throws Exception
  {
    ExecuteStep.executeList(execNames);
  }
  
  protected abstract String getLoadDataInfileQuery();
  
  public Dialect getDialect()
  {
    return dialect;
  }
  
  /** Load static common configuration. */
  public static void loadConfig()
  {
    dialect = GlobalConfiguration.getCSVDialect();
    if (dialect == null)
      dialect = Dialects.Default;
  }
}
