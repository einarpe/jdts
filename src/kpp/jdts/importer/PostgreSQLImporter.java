package kpp.jdts.importer;

import java.io.UnsupportedEncodingException;

import kpp.jtds.core.Logger;
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
    sb.append('(').append(columnsFromResultSet.toString()).append(')');
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
  protected Object obj2str(Object object, String columnType)
  {
    if (object != null && object instanceof String)
      try
      {
        byte[] bytes = object.toString().getBytes("windows-1250");
        for (int i = 0; i < bytes.length; i++)
          if (bytes[i] == 0) // zero bytes are not allowed; replace them with space characters
            bytes[i] = 32;
        
        return String.format("'%s'", new String(bytes)
          .replace("\\", "\\\\")
          .replace("'", "\\'"));
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
    return String.format("Truncate Table Only %s", config.getProperty(CP_INTO));
  }
}
