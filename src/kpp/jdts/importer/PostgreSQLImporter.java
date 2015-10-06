package kpp.jdts.importer;

import kpp.jdts.csv.dialect.Dialect;
import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.core.Step;

public class PostgreSQLImporter extends Importer
{
  static
  {
    appendLastSemicolon = false;
  }
  
  public PostgreSQLImporter(Step step)
  {
    super(step);
  }

  @Override
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    sb.append("COPY ").append(config.getProperty(CP_INTO));
    sb.append('(').append(getColumnsFromResultSet()).append(')');
    sb.append(" FROM ");
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("'").append(path).append("' ");
    
    sb.append("NULL '\\N' ");
    sb.append("QUOTE '''' ");
    sb.append("ESCAPE '\\' ");
    sb.append("DELIMITER ';' CSV ");
    sb.append("ENCODING 'windows-1250' ");
    
    return sb.toString();
  }
  
  @Override
  protected String getTruncateQuery()
  {
    return String.format("Truncate Table Only %s", config.getProperty(CP_INTO));
  }

  @Override
  public Dialect getDialect()
  {
    return Dialects.PostgreSQL;
  }
}
