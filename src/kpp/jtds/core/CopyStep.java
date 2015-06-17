package kpp.jtds.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class CopyStep extends Step
{ 
  final static SimpleDateFormat shortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  
  final static SimpleDateFormat longDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  
  private String tableName;
  
  private String sqlWhere;
  
  private String columns;
  
  private boolean truncate;
  
  private ArrayList<ExprColumn> exprColumns = new ArrayList<>();
  
  
  private CopyStep() { }
  
  private int rows = 0;

  @Override
  public void execute(DTS dts) throws Exception
  {
    System.out.println("Start COPY step. Current table is " + tableName);
    
    StringBuilder sb = prepare(dts);
    if (rows > 0)
    {
      File tmp = saveCSV(sb);
      insertCSV(dts, tmp);
    }
    
    System.out.println("Done, total " + rows + " rows. ");
  }

  /** 
   * Ściąganie danych ze źródła, celem przygotowania pliku CSV
   * @return StringBuilder przechowywujący zawartość pliku
   */
  private StringBuilder prepare(DTS dts) throws Exception
  {
    System.out.print("Preparing data... ");
    
    String columnList = getColumns(true);
    String sqlQuery = String.format("Select %s from %s", columnList, tableName);
    if (sqlWhere != null && sqlWhere.length() > 0)
      sqlQuery += String.format(" Where %s", sqlWhere);
    
    PreparedStatement stmt = dts.getSrcConn().prepareStatement(sqlQuery);
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    StringBuilder sb = new StringBuilder();
    int i = 0;
    while (rs.next())
    {
      for (int k = 1; k <= columnCount; k++)
        sb.append(obj2str(rs.getObject(k), rsmd.getColumnTypeName(k))).append(";");
      
      sb.append("\r\n");
      i++;
    }
    stmt.close();
    rows = i;
    System.out.println("OK.");
    return sb;
  }
  
  /**
   * Pobranie listy kolumn
   * @param source - czy lista kolumn do źródła? uwzględniane wtedy są funkcje w transformacjach
   * @return lista kolumn jako pojedynczy String, w którym każda kolumna oddzielona jest przecinkiem
   */
  private String getColumns(boolean source)
  {
    StringBuilder result = new StringBuilder(columns);
    if (exprColumns.size() > 0)
    {
      result.append(",");
      for (int i = 0, l = exprColumns.size(); i < l; i++)
      {
        ExprColumn tc = exprColumns.get(i);
        if (source)
          result.append(tc.expression).append(" As ");
        
        result.append(tc.columnName);
        if (i < l - 1)
          result.append(",");
      }
    }
    return result.toString();
  }
  
  /**
   * Zapis treści pliku CSV do tymczasowego pliku na dysku.
   * @param sb - obiekt zawierający treść pliku
   * @return uchwyt do tymczasowego pliku na dysku 
   * @throws Exception 
   */
  private File saveCSV(StringBuilder sb) throws Exception
  {
    File tmp = File.createTempFile("jdts", ".csv");
    try(OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(tmp)))
    {
      osw.write(sb.toString());
      osw.close();
    }
    tmp.deleteOnExit();
    return tmp;
  }
  
  /**
   * Import fizycznego pliku CSV do bazy danych
   * @param dts 
   * @param tmp - uchwyt do pliku tymczasowego
   * @throws Exception
   */
  private void insertCSV(DTS dts, File tmp) throws Exception
  {
    StringBuilder sb = new StringBuilder();
    String sqlQuery = "";
    if (truncate)
    {
      System.out.print("Truncating... ");
      sqlQuery = String.format("Truncate Table %s", tableName);
      dts.getDestConn().prepareStatement(sqlQuery).execute();
      System.out.println("OK");
    }
    
    System.out.print("Inserting... ");
    String columnList = getColumns(false);
    
    String path = tmp.getAbsolutePath().replace("\\", "\\\\");
    sb.append("Load Data Local Infile '").append(path).append("' ");
    sb.append("Into Table ").append(tableName).append(" ");
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
    result.tableName = step.getAttribute("table");
    result.columns = step.getAttribute("columns");
    result.sqlWhere = step.getAttribute("where");
    result.truncate = step.getAttribute("truncate").equalsIgnoreCase("true");
    
    NodeList children = step.getElementsByTagName("*");
    for (int i = 0, l = children.getLength(); i < l; i++)
    {
      Element child = (Element)children.item(i);
      if (!child.getNodeName().equalsIgnoreCase("column"))
        continue;
      
      ExprColumn tc = new ExprColumn();
      tc.columnName = child.getAttribute("name");
      tc.expression = child.getAttribute("expression");
      result.exprColumns.add(tc);
    }
    
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy table=%s where=%s", tableName, sqlWhere);
  }
  
  /** Kolumna z transformacją */
  static class ExprColumn
  {
    public String columnName;
    public String expression;
    
    public String toString()
    {
      return String.format("%s As %s", expression, columnName);
    }
  }

}
