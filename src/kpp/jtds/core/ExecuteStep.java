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

  @SuppressWarnings("resource")
  @Override
  public void execute() throws Exception
  {
    Connection conn = (on.equalsIgnoreCase("destination")) ? dts.getDestConnection() : dts.getSourceConnection();
    if (procedure != null && !procedure.isEmpty())
    {
      System.out.print(String.format("Executing stored procedure %s on %s connection... ", procedure, on));
      CallableStatement cs = conn.prepareCall("{call " + procedure + "}");
      cs.execute();
      cs.close();
    }
    
    if (query != null && !query.isEmpty())
    {
      System.out.print(String.format("Executing query on %s connection... ", on));
      PreparedStatement ps = conn.prepareStatement(query);
      ps.execute();
      ps.close();
    }
    System.out.println("OK.");
  }
  
  public static ExecuteStep create(Element el, DTS dts)
  {
    ExecuteStep result = new ExecuteStep(dts);
    result.procedure = el.getAttribute("procedure");
    result.on = el.getAttribute("on");
    result.query = el.getTextContent();
    return result;
  }

}
