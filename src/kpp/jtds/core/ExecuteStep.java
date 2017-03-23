package kpp.jtds.core;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Element;

public class ExecuteStep extends Step
{
  /** Mapping of steps names to their selves. */
  private static Map<String, ExecuteStep> stepsMap = new HashMap<>();
  
  /** Empty step which does nothing. */
  public final static ExecuteStep Empty = new ExecuteStep();
  
  /** Name of procedure to execute. */
  private String procedure;
  
  /** SQL query to execute. */
  private String query;
  
  /** On which connection should be procedure or query executed (allowed values: "source" or "destination"). */
  private String on = "destination";
  
  /** Should fail on error? */
  private boolean failOnError = true;
  
  /** Defer executing? */
  private boolean defer = false;
  
  /** Name of this execution command. */
  private String name = "";
  
  public void execute(DTS dts) throws Exception
  {
    execute(dts, false);
  }
  
  /**
   * Execute query or procedure.
   * @param ignoreDeferFlag - method called outside main process loop (eg. when calling before/after some copy step).
   * @throws Exception when something bad occurs
   */
  public void execute(DTS dts, boolean ignoreDeferFlag) throws Exception
  {
    if (this == Empty)
      return;
    
    boolean deferred = !ignoreDeferFlag && defer; 
    
    Logger.info(String.format("Executing query %s... %s.", 
        name.isEmpty() ? "<noname>" : name, 
        deferred ? "deffered" : "" ));
    
    if (deferred)
      return;
    
    try
    {
      Connection conn = (on.equalsIgnoreCase("destination")) ? dts.getDestConnection() : dts.getSourceConnection();
      execProcedure(conn);
      execQuery(conn);
      
      Logger.info("Executing query done.");
    }
    catch (Exception ex)
    {
      Logger.error(ex.getMessage());
      
      if (failOnError)
        throw ex;
    }
  }

  private void execQuery(Connection conn) throws SQLException
  {
    if (query == null || query.isEmpty())
      return;
    
    Logger.info(String.format("Query execute on %s connection... ", on));
    Logger.debug(query);
    
    try (PreparedStatement ps = conn.prepareStatement(query))
    {
      ps.execute();
    }
  }

  private void execProcedure(Connection conn) throws SQLException
  {
    if (procedure == null || procedure.isEmpty())
      return;
    
    Logger.info(String.format("Stored procedure call %s on %s connection... ", procedure, on));
    try (CallableStatement cs = conn.prepareCall("{call " + procedure + "}"))
    {
      cs.execute();
    }
  }
  
  /**
   * Returns instance of ExecuteStep based on XML element data.
   * @param el - xml element
   * @param dts - DTS object
   * @return created step
   */
  public static ExecuteStep create(Element el)
  {
    ExecuteStep result = new ExecuteStep();
    result.procedure = el.getAttribute("procedure");
    result.on = el.getAttribute("on");
    result.query = el.getTextContent();
    result.failOnError = Boolean.parseBoolean(el.getAttribute("failonerror"));
    result.defer = Boolean.parseBoolean(el.getAttribute("defer"));
    result.name = el.getAttribute("name");
    
    addToMap(result.name, result);
    return result;
  }
   
  private static void addToMap(String name, ExecuteStep step)
  {
    if (!name.isEmpty())
      stepsMap.put(name, step);
  }
  
  public static ExecuteStep get(String name)
  {
    return stepsMap.get(name);
  }
}
