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
  final static SimpleDateFormat shortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  
  final static SimpleDateFormat longDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  static class ConfigData
  {
    public String query;
    
    public String into;
    
    public boolean truncate;
  }
  
  ConfigData config = new ConfigData();
  
  static int number = 0;
  
  private CopyStep() { }
  
  private int rows = 0;
  
  private StringBuilder columnsFromResultSet = new StringBuilder();

  @Override
  public void execute(DTS dts) throws Exception
  {
    System.out.println(String.format("Copy step #%d into table %s is running now.", ++number, config.into));
    
    FileStringBuilder fsb = prepare(dts);
    if (rows > 0)
      insertCSV(dts, fsb);
    
    System.out.println("Done, total " + rows + " rows. ");
  }

  /** 
   * Ściąganie danych ze źródła, celem przygotowania pliku CSV
   * @return StringBuilder przechowywujący zawartość pliku
   */
  private FileStringBuilder prepare(DTS dts) throws Exception
  {
    System.out.print("Preparing data... ");
    
    PreparedStatement stmt = dts.getSrcConn().prepareStatement(config.query);
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    
    for (int k = 1; k <= columnCount; k++)
    {
      columnsFromResultSet.append(rsmd.getColumnLabel(k));
      if (k < columnCount)
        columnsFromResultSet.append(",");
    }
    
    FileStringBuilder fsb = new FileStringBuilder();
    int i = 0;
    while (rs.next())
    {
      for (int k = 1; k <= columnCount; k++)
        fsb.append(obj2str(rs.getObject(k), rsmd.getColumnTypeName(k))).append(";");
      
      fsb.append("\r\n");
      i++;
    }
    fsb.close();
    stmt.close();
    rows = i;
    System.out.println("OK.");
    return fsb;
  }
  
  /**
   * Import fizycznego pliku CSV do bazy danych
   * @param dts 
   * @param tmp - uchwyt do pliku tymczasowego
   * @throws Exception
   */
  private void insertCSV(DTS dts, FileStringBuilder fsb) throws Exception
  {
    StringBuilder sb = new StringBuilder();
    String sqlQuery = "";
    if (config.truncate)
    {
      System.out.print("Truncating... ");
      sqlQuery = String.format("Truncate Table %s", config.into);
      dts.getDestConn().prepareStatement(sqlQuery).execute();
      System.out.println("OK");
    }
    
    System.out.print("Inserting... ");
    String columnList = columnsFromResultSet.toString();
    
    String path = fsb.getFile().getAbsolutePath().replace("\\", "\\\\");
    sb.append("Load Data Local Infile '").append(path).append("' ");
    sb.append("Into Table ").append(config.into).append(" ");
    sb.append("Fields Terminated By ';' Enclosed By '' Escaped by '\\\\' ");
    sb.append("(").append(columnList).append(") ");
    
    sqlQuery = sb.toString();
    PreparedStatement ps = dts.getDestConn().prepareStatement(sqlQuery);
    ps.execute();
    ps.close();
    
    System.out.println("OK");
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
      return shortDateFormat.format((Date)object);
    
    if (columnType.equalsIgnoreCase("datetime"))
      return longDateFormat.format((Date)object);
    
    return object;
  }
  
  /**
   * Utworzenie instacji CopyStep na podstawie elementu XML.
   * @param step - element xml
   * @return 
   */
  public static CopyStep fromXml(Element step)
  {
    CopyStep result = new CopyStep();
    result.config.into = step.getAttribute("into");
    NodeList queries = step.getElementsByTagName("query");
    if (queries.getLength() > 0)
      result.config.query = ((Element)queries.item(0)).getTextContent();
    
    result.config.truncate = step.getAttribute("truncate").equalsIgnoreCase("true");
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy into=%s %s", config.into, config.truncate ? "with truncate" : "");
  }
}
