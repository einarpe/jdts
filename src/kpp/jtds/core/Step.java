package kpp.jtds.core;

import java.sql.SQLException;

import org.w3c.dom.Element;

public abstract class Step
{
  
  protected DTS dts = null;
  
  protected Step(DTS dts)
  {
    this.dts = dts;
  }
  
  public abstract void execute() throws Exception;

  public static Step create(Element step, DTS dts) throws Exception
  {
    if (step == null)
      return null;
    
    String nodeName = step.getNodeName();
    switch (nodeName)
    {
      case "copy": return CopyStep.create(step, dts);
      case "exec": return ExecuteStep.create(step, dts);
    }
    
    throw new Exception("Unrecognized step " + step.getNodeName());
  }
  
  /** 
   * Wykonuje polecenie SQL na połączeniu docelowym
   * @param sqlQuery - zapytanie
   * @throws SQLException
   */
  protected void destinationExecStatement(String sqlQuery) throws SQLException
  {
    dts.getDestConnection().prepareStatement(sqlQuery).execute();
  }
  
  /** 
   * Wykonuje polecenie SQL na połączeniu źródłowym
   * @param sqlQuery - zapytanie
   * @throws SQLException
   */
  protected void sourceExecStatement(String sqlQuery) throws SQLException
  {
    dts.getSourceConnection().prepareStatement(sqlQuery).execute();
  }
}
