package kpp.jtds.core;

import org.w3c.dom.Element;

public abstract class Step
{
  public abstract void execute(DTS dts) throws Exception;

  public static Step fromXml(Element step)
  {
    if (step == null)
      return null;
    
    String nodeName = step.getNodeName();
    switch (nodeName)
    {
      case "copy": return CopyStep.fromXml(step);
      case "exec": return ExecuteStep.fromXml(step);      
    }
    
    return null;
  }
}
