package kpp.jdts.importer;

import kpp.jtds.core.Step;

public class MySQLImporter extends Importer
{
  
  static
  {
    quotedColumns.add("separator");
    quotedColumns.add("order");
    
    quoteColumnChar = "`";
  }
  
  public MySQLImporter(Step step)
  {
    super(step);
    
    
  }
  
  /** Return LOAD DATA INFILE query. Query is based on temporary file location and list of columns from source ResultSet. */
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("Load Data Local Infile '").append(path).append("' ");
    
    if (!config.getProperty(CP_BEHAVIOUR).isEmpty())
      sb.append(config.getProperty(CP_BEHAVIOUR)).append(' ');
    
    sb.append("Into Table ").append(config.getProperty(CP_INTO)).append(' ');
    sb.append("Fields Terminated By ';' Enclosed By '' Escaped by '\\\\' Lines Terminated By '\r\n' ");
    sb.append('(').append(columnsFromResultSet.toString()).append(')');
    
    return sb.toString();
  }
}
