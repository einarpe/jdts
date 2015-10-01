package kpp.jdts.importer;

import kpp.jtds.core.Step;

public class PostgreSQLImporter extends Importer
{
  public PostgreSQLImporter(Step step)
  {
    super(step);
    appendLastSemicolon = false;
  }

  @Override
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    sb.append("Copy ").append(config.getProperty(CP_INTO));
    sb.append('(').append(columnsFromResultSet.toString()).append(')');
    sb.append(" From ");
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("'").append(path).append("' ");
    
    sb.append("NULL '\\N' ");
    sb.append("QUOTE '''' ");
    sb.append("ESCAPE '\\' ");
    sb.append("DELIMITER ';' CSV ");
    
    return sb.toString();
  }
  
  @Override
  protected String getTruncateQuery()
  {
    return String.format("Truncate Table %s Cascade", config.getProperty(CP_INTO));
  }

}
