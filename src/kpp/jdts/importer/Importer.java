package kpp.jdts.importer;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import kpp.jdts.csv.FileStringBuilder;
import kpp.jdts.csv.dialect.Dialect;
import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.GlobalConfiguration;
import kpp.jtds.GlobalConfiguration.DialectConfig;
import kpp.jtds.core.CopyStep;
import kpp.jtds.core.DTS;
import kpp.jtds.core.Logger;

import com.google.common.base.Joiner;

public abstract class Importer
{
  protected DTS dts;
  
  protected CopyStep step;
  
  /** List of columns (or to be precise - aliases) from resulting query. */
  protected List<String> columnsFromResultSet;
  
  protected FileStringBuilder fsb;
  
  /** Some properties of importer. */
  protected Properties config = new Properties();

  /** Dialect to use when saving data to CSV file. */
  protected static Dialect dialect = Dialects.Default;
  
  /** List of nasty column names which should be quoted. */
  protected static Set<String> quotedColumns = new HashSet<String>();
  
  /** Character using for quoting nasty columns, so RMDBs will not complain about it. */
  protected static String quoteColumnChar = "";

  /** If there should be last semicolon on each line of CSV? */
  protected static boolean appendLastSemicolon = true;
  
  public Importer(DTS dts, CopyStep step)
  {
    this.dts = dts;
    this.step = step;
  }
  
  /** 
   * Preparation of CSV file. Function reads data based on query and writes it to temporary CSV file.
   * @return rows read from source database
   */
  public int prepare() throws Exception
  {
    Logger.info("Preparing data for table ", step.getInto(), "... ");
    
    int rows = 0;
    try (PreparedStatement stmt = dts.getSourceConnection().prepareStatement(step.getQuery()))
    {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      
      readResultSetColumns(rsmd);
      rows = readFileStringBuilder(rs, rsmd);
    }
    
    Logger.info("Preparation data ended. Row count for query: ", rows);
    return rows;
  }

  private int readFileStringBuilder(ResultSet rs, ResultSetMetaData rsmd) throws IOException, SQLException, Exception
  {
    int rows = 0;
    int totalColumnCount = rsmd.getColumnCount();
    Dialect dialect = getDialect();
    try (FileStringBuilder fsb = new FileStringBuilder())
    {
      while (rs.next())
      {
        for (int columnNo = 1; columnNo <= totalColumnCount; columnNo++)
        {
          fsb.append(
              dialect.objectToString(
                  rs.getObject(columnNo), 
                  rsmd.getColumnTypeName(columnNo)));
          
          if (columnNo == totalColumnCount)
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
      this.fsb = fsb;
    }
    return rows;
  }

  private void readResultSetColumns(ResultSetMetaData rsmd) throws SQLException
  {
    int columnCount = rsmd.getColumnCount();
    columnsFromResultSet = new LinkedList<>();
    for (int k = 1; k <= columnCount; k++)
    {
      String columnName = quote(rsmd.getColumnLabel(k));
      columnsFromResultSet.add(columnName);
    }
  }
  
  @Override
  public String toString()
  {
    return String.format("into=%s ", step.getInto());
  }
  
  /**
   * We have got not empty temporary file with CSV contents now. 
   * So perform merging it with destination table. 
   * @throws Exception
   */
  public void insert() throws Exception
  {
    String sqlQuery = "";
    if (step.getTruncate())
    {
      Logger.info("Truncating... "); 
      sqlQuery = getTruncateQuery(); 
      Logger.debug(sqlQuery);
      
      dts.executeStatementDestination(sqlQuery);
      Logger.info("Truncating done.");
    }
    
    Logger.info("Inserting... ");
    sqlQuery = getLoadDataInfileQuery(); 
    Logger.debug(sqlQuery);
    
    dts.executeStatementDestination(sqlQuery);
    
    Logger.info("Inserting done.");
  }

  /** Return TRUNCATE TABLE or DELETE FROM query. */
  protected String getTruncateQuery()
  {
    return String.format("Truncate Table %s", step.getInto());
  }
  
  protected String quote(String columnName)
  {
    if (quotedColumns.contains(columnName.toLowerCase()))
      return String.format("%s%s%s", quoteColumnChar, columnName, quoteColumnChar);
    
    return columnName;
  }
  
  
  protected abstract String getLoadDataInfileQuery();
  
  public Dialect getDialect()
  {
    return dialect;
  }
  
  protected String getColumnsFromResultSet()
  {
    return Joiner.on(',').join(columnsFromResultSet);
  }
  
  /** Load static common configuration. */
  public static void loadConfig()
  {
    DialectConfig conf = GlobalConfiguration.getCSVDialect();
    
    dialect = (conf.UsingDialect == null) ? Dialects.Default : conf.UsingDialect;  
    dialect.setConfig(conf);
  }
}
