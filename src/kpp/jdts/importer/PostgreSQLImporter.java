package kpp.jdts.importer;

import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.core.CopyStep;
import kpp.jtds.core.DTS;

public class PostgreSQLImporter extends Importer
{
  public PostgreSQLImporter(DTS dts, CopyStep step)
  {
    super(dts, step);
  }

  static
  {
    init();
  }

  private static void init()
  {
    appendLastSemicolon = false;
    dialect = Dialects.PostgreSQL;
  }

  @Override
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    sb.append("COPY ").append(step.getInto());
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
    return String.format("Truncate Table Only %s", step.getInto());
  }

}
