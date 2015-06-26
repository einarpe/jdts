package kpp.jtds.core;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.Date;

import kpp.jdts.csv.FileStringBuilder;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class CopyStep extends Step
{ 
  final static SimpleDateFormat ShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  
  final static SimpleDateFormat LongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  /** Konfiguracja na podstawie odczytanego elementu XML */
  static class ConfigData
  {
    public String query;
    
    public String into;
    
    public String behaviour;
    
    public boolean truncate;
  }
  
  ConfigData config = new ConfigData();
  
  static int number = 0;
  
  public CopyStep(DTS dts)
  {
    super(dts);
  }
  
  private StringBuilder columnsFromResultSet = new StringBuilder();
  
  private FileStringBuilder fsb;

  @Override
  public void execute() throws Exception
  {
    System.out.println(String.format("Copy step #%d into table %s is running now.", ++number, config.into));
    
    int rows = prepare();
    if (rows > 0)
      insertCSV();
    
    System.out.println("Done, total " + rows + " rows. ");
  }

  /** 
   * Ściąganie danych ze źródła, celem przygotowania pliku CSV
   * @return StringBuilder przechowywujący zawartość pliku
   */
  private int prepare() throws Exception
  {
    System.out.print("Preparing data... ");
    
    PreparedStatement stmt = dts.getSourceConnection().prepareStatement(config.query);
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    
    for (int k = 1; k <= columnCount; k++)
    {
      columnsFromResultSet.append(rsmd.getColumnLabel(k));
      if (k < columnCount)
        columnsFromResultSet.append(',');
    }
    
    fsb = new FileStringBuilder();
    
    int i = 0;
    while (rs.next())
    {
      for (int k = 1; k <= columnCount; k++)
      {
        fsb.append(obj2str(rs.getObject(k), rsmd.getColumnTypeName(k))).append(";");
      }
      
      fsb.append("\r\n");
      i++;
    }
    fsb.close();
    stmt.close();
    
    System.out.println("OK.");
    return i;
  }
  
  /**
   * Import fizycznego pliku CSV do bazy danych
   * @param dts 
   * @param tmp - uchwyt do pliku tymczasowego
   * @throws Exception
   */
  private void insertCSV() throws Exception
  {
    String sqlQuery = "";
    if (config.truncate)
    {
      System.out.print("Truncating... ");
      sqlQuery = getTruncateQuery(); 
      destinationExecStatement(sqlQuery); 
      System.out.println("OK");
    }
    
    System.out.print("Inserting... ");
    sqlQuery = getLoadDataInfileQuery();
    destinationExecStatement(sqlQuery);
    System.out.println("OK");
  }
  
  /** Pobranie zapytania dodającego dane do tabeli na podstawie pliku csv */
  protected String getLoadDataInfileQuery()
  {
    StringBuilder sb = new StringBuilder();
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("Load Data Local Infile '").append(path).append("' ");
    
    if (!config.behaviour.isEmpty())
      sb.append(config.behaviour).append(' ');
    
    sb.append("Into Table ").append(config.into).append(' ');
    sb.append("Fields Terminated By ';' Enclosed By '' Escaped by '\\\\' Lines Terminated By '\r\n' ");
    sb.append('(').append(columnsFromResultSet.toString()).append(')');
    
    return sb.toString();
  }
  
  /** Pobranie zapytania usuwającego dane z docelowej tabeli */
  protected String getTruncateQuery()
  {
    return String.format("Delete From %s", config.into);
  }

  /**
   * Konwersja obiektu do postaci zrozumiałej przez MySQLowy czytnik CSV
   * @param object - obiekt 
   * @param columnType - typ kolumny
   * @return ciąg znaków zapisany jako wartość kolumny CSV
   */
  private Object obj2str(Object object, String columnType)
  {
    if (object == null)
      return "\\N"; // szpecjal for mysql
    
    if (object instanceof String)
      return object.toString().replace("\"", "\\\"");
    
    if (columnType.equalsIgnoreCase("date"))
      return ShortDateFormat.format((Date)object);
    
    if (columnType.equalsIgnoreCase("datetime"))
      return LongDateFormat.format((Date)object);
    
    return object;
  }
  
  /**
   * Utworzenie instacji CopyStep na podstawie elementu XML.
   * @param step - element xml
   * @return 
   */
  public static CopyStep create(Element step, DTS dts)
  {
    CopyStep result = new CopyStep(dts);
    result.dts = dts;
    result.config.into = step.getAttribute("into");
    NodeList queries = step.getElementsByTagName("query");
    if (queries.getLength() > 0)
      result.config.query = ((Element)queries.item(0)).getTextContent();
    
    result.config.truncate = step.getAttribute("truncate").equalsIgnoreCase("true");
    result.config.behaviour = step.getAttribute("behaviour");
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy into=%s %s", config.into, config.truncate ? "with truncate" : "");
  }
}
