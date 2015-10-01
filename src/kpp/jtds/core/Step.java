package kpp.jtds.core;

import java.sql.SQLException;

import org.w3c.dom.Element;

/** 
 * Some step to perform on source/destination database... Base class. 
 */
public abstract class Step
{
  
  /** DTS object and its data, which we are working on */
  protected DTS dts = null;
  
  protected Step(DTS dts)
  {
    this.dts = dts;
  }
  
  /** Execute this step */
  public abstract void execute() throws Exception;

  /** Factory to create step from XML element */
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
  
  public DTS getDTS()
  {
    return dts;
  }
}
