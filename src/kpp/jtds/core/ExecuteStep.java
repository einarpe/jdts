package kpp.jtds.core;

import java.sql.CallableStatement;
import java.sql.Connection;

import org.w3c.dom.Element;

public class ExecuteStep extends Step
{
  private String procedure;
  
  private String on = "destination";

  @SuppressWarnings("resource")
  @Override
  public void execute(DTS dts) throws Exception
  {
    if (procedure == null || procedure.isEmpty())
      return;
    
    System.out.print(String.format("Executing stored procedure %s on %s connection... ", procedure, on));
    Connection conn = (on.equalsIgnoreCase("destination")) ? dts.getDestConn() : dts.getSrcConn();
    CallableStatement cs = conn.prepareCall("{call " + procedure + "}");
    cs.execute();
    cs.close();
    System.out.println("OK.");
  }
  
  public static ExecuteStep fromXml(Element el)
  {
    ExecuteStep result = new ExecuteStep();
    result.procedure = el.getAttribute("procedure");
    result.on = el.getAttribute("on");
    return result;
  }

}
