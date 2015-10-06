package kpp.jdts.csv.dialect;

import java.io.UnsupportedEncodingException;

import kpp.jtds.core.Logger;

public class Dialects
{
  /** MySQL dialect. */
  public final static Dialect MySQL = new MySqlDialect();
  
  /** PostgreSQL dialect. */
  public final static Dialect PostgreSQL = new PgSqlDialect();
  
  /** Default which is by default MySQL dialect. */
  public final static Dialect Default = new MySqlDialect();
  
  private static class MySqlDialect extends Dialect
  {
  }
  
  private static class PgSqlDialect extends Dialect
  {
    @Override
    public Object objectToString(Object object, String columnType) throws Exception
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
      
      return super.objectToString(object, columnType);
    }
  }
  
}
