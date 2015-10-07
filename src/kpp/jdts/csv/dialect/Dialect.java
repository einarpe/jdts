package kpp.jdts.csv.dialect;

import java.text.SimpleDateFormat;
import java.util.Date;

import kpp.jtds.GlobalConfiguration.DialectConfig;

public abstract class Dialect
{
  public final static SimpleDateFormat ShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  
  public final static SimpleDateFormat LongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  private String quoteCharacter;

  private String delimiter;

  private String nullCharacter;

  private String escapeCharacter;
  
  /**
   * Convert given object to string readable by MYSQL LOAD DATA INFILE or some other similar query.
   * @param object - object to convert
   * @param columnType - column type from ResultSetMetaData
   * @return string readable by RDMBs CSV parser
   */
  public Object objectToString(Object object, String columnType) throws Exception
  {
    if (object == null)
      return "\\N"; // mysql default null identifier
    
    if (object instanceof String)
      return object.toString().replace("\"", "\\\"").replace(";", "\\;");
    
    if (columnType.equalsIgnoreCase("date"))
      return ShortDateFormat.format((Date)object);
    
    if (columnType.equalsIgnoreCase("datetime"))
      return LongDateFormat.format((Date)object);
    
    return object;
  }
  
  public String getQuoteCharacter()
  {
    return quoteCharacter;
  }
  
  public String getNullCharacter()
  {
    return nullCharacter;
  }
  
  public String getEscapeCharacter()
  {
    return escapeCharacter;
  }
  
  public String getDelimiter()
  {
    return delimiter;
  }

  public void setConfig(DialectConfig conf)
  {
    quoteCharacter = conf.QuoteCharacter != null ? conf.QuoteCharacter : "'";
    delimiter = conf.Delimiter != null ? conf.Delimiter : ";";
    nullCharacter = "\\N";
    escapeCharacter = "\\";
  }
}
