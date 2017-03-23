package kpp.jtds.core;

import java.sql.SQLException;

import org.w3c.dom.Element;

/** 
 * This is base, abstract class for all of steps. 
 */
public abstract class Step
{
  /** 
   * Execute this step.
   * @throws Exception - when something goes wrong 
   */
  public abstract void execute(DTS dts) throws Exception;
  
  /** 
   * Executes given query on destination database
   * @param sqlQuery - query to execute
   * @throws SQLException
   */
  public void destinationExecStatement(DTS dts, String sqlQuery) throws SQLException
  {
    dts.getDestConnection().prepareStatement(sqlQuery).execute();
  }
  
  /** 
   * Executes given query on source database
   * @param sqlQuery - query to execute
   * @throws SQLException
   */
  public void sourceExecStatement(DTS dts, String sqlQuery) throws SQLException
  {
    dts.getSourceConnection().prepareStatement(sqlQuery).execute();
  }
  

  /** Factory to create step from XML element */
  public static Step create(Element step) throws Exception
  {
    if (step == null)
      return null;
    
    String nodeName = step.getNodeName();
    switch (nodeName)
    {
      case "copy": return CopyStep.create(step);
      case "exec": return ExecuteStep.create(step);
    }
    
    throw new Exception("Unrecognized step " + step.getNodeName());
  }
}
