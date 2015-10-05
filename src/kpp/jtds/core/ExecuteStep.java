package kpp.jtds.core;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.w3c.dom.Element;

public class ExecuteStep extends Step
{
  private String procedure;
  
  private String query;
  
  private String on = "destination";

  public ExecuteStep(DTS dts)
  {
    super(dts);
  }
  
  @Override
  public void execute() throws Exception
  {
    Logger.info("Executing query starting... "); 
    
    @SuppressWarnings("resource")
    Connection conn = (on.equalsIgnoreCase("destination")) ? dts.getDestConnection() : dts.getSourceConnection();
    if (procedure != null && !procedure.isEmpty())
    {
      Logger.info(String.format("Stored procedure call %s on %s connection... ", procedure, on));
      try (CallableStatement cs = conn.prepareCall("{call " + procedure + "}"))
      {
        cs.execute();
      }
    }
    
    if (query != null && !query.isEmpty())
    {
      Logger.info(String.format("Query execute on %s connection... ", on));
      Logger.debug(query);
      
      try (PreparedStatement ps = conn.prepareStatement(query))
      {
        ps.execute();
      }
    }
    Logger.info("Executing query done.");
  }
  
  /**
   * Returns instance of ExecuteStep based on XML element data.
   * @param el - xml element
   * @param dts - DTS object
   * @return created step
   */
  public static ExecuteStep create(Element el, DTS dts)
  {
    ExecuteStep result = new ExecuteStep(dts);
    result.procedure = el.getAttribute("procedure");
    result.on = el.getAttribute("on");
    result.query = el.getTextContent();
    return result;
  }

}
