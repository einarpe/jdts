package kpp.jdts.importer;

import java.io.UnsupportedEncodingException;

import kpp.jtds.core.Logger;
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
    sb.append("ENCODING 'windows-1250' ");
    
    return sb.toString();
  }
  
  @Override
  protected Object obj2str(Object object, String columnType)
  {
    if (object != null && object instanceof String)
      try
      {
        return new String(object.toString().getBytes("windows-1250"));
      }
      catch (UnsupportedEncodingException e)
      {
        Logger.error(e.getMessage()); 
      }
    
    return super.obj2str(object, columnType);
  }
  
  @Override
  protected String getTruncateQuery()
  {
    return String.format("Truncate Table %s Cascade", config.getProperty(CP_INTO));
  }
}
