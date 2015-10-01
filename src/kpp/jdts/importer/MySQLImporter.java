package kpp.jdts.importer;

import java.util.Date;

import kpp.jtds.core.Step;

public class MySQLImporter extends Importer
{
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

}
