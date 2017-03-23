package kpp.jdts.importer;

import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.core.CopyStep;
import kpp.jtds.core.DTS;

public class MySQLImporter extends Importer
{

  public MySQLImporter(DTS dts, CopyStep step)
  {
    super(dts, step);
  }

  static
  {
    init();
  }

  private static void init()
  {
    quotedColumns.add("separator");
    quotedColumns.add("order");
    
    quoteColumnChar = "`";
    
    dialect = Dialects.MySQL;
  }
  
  /** Return LOAD DATA INFILE query. Query is based on temporary file location and list of columns from source ResultSet. */
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("Load Data Local Infile '").append(path).append("' ");
    
    
    if (!step.getBehaviour().isEmpty())
      sb.append(step.getBehaviour()).append(' ');
    
    sb.append("Into Table ").append(step.getInto()).append(' ');
    sb.append("Fields Terminated By ';' Enclosed By '' Escaped by '\\\\' Lines Terminated By '\r\n' ");
    sb.append('(').append(getColumnsFromResultSet()).append(')');
    
    return sb.toString();
  }

}
