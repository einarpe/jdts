package kpp.jtds.core;

import java.sql.SQLException;

import org.w3c.dom.Element;

/** 
 * This is base, abstract class for all of steps. 
 */
public abstract class Step
{
  
  /** DTS object and its data, which we are working on */
  protected DTS dts = null;
  
  /** 
   * Execute this step.
   * @throws Exception - when something goes wrong 
   */
  public abstract void execute() throws Exception;
  
  public void setDTS(DTS dts)
  {
    this.dts = dts;
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
  
  /** 
   * Executes given query on destination database
   * @param sqlQuery - query to execute
   * @throws SQLException
   */
  public void destinationExecStatement(String sqlQuery) throws SQLException
  {
    dts.getDestConnection().prepareStatement(sqlQuery).execute();
  }
  
  /** 
   * Executes given query on source database
   * @param sqlQuery - query to execute
   * @throws SQLException
   */
  public void sourceExecStatement(String sqlQuery) throws SQLException
  {
    dts.getSourceConnection().prepareStatement(sqlQuery).execute();
  }
  
  /** Return DTS linked with this step. */
  public DTS getDTS()
  {
    return dts;
  }
}
