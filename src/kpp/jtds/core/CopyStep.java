package kpp.jtds.core;

import kpp.jdts.importer.Importer;
import kpp.jdts.importer.ImporterFactory;

import org.w3c.dom.Element;

public class CopyStep extends Step
{ 
  static int number = 0;
  
  private String[] executeBefore = new String[0];
  
  private String[] executeAfter = new String[0];
  
  public CopyStep(DTS dts)
  {
    super(dts);
    ++number;
  }
  
  private Importer importer;

  @Override
  public void execute() throws Exception
  {
    importer.executeStepExecute(executeBefore);
    
    int rows = importer.prepare();
    if (rows > 0)
      importer.insert();
    
    importer.executeStepExecute(executeAfter);
  }
  
  /**
   * Returns instance of CopyStep class based on XML element.
   * @param element - xml element
   * @return 
   * @throws Exception 
   */
  public static CopyStep create(Element element, DTS dts) throws Exception
  {
    CopyStep result = new CopyStep(dts);
    result.dts = dts;
    result.importer = ImporterFactory.newInstance(result);
    result.importer.setPropertiesFromXml(element);
    
    Element execute = XmlUtils.getFirst(element, "exec");
    if (execute != null)
    {
      result.executeBefore = execute.getAttribute("before").split(",");
      result.executeAfter = execute.getAttribute("after").split(",");
    }
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy importer=%s", importer);
  }
}
