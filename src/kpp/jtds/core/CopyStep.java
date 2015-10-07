package kpp.jtds.core;

import java.util.ArrayList;

import kpp.jdts.importer.Importer;
import kpp.jdts.importer.ImporterFactory;

import org.w3c.dom.Element;

import com.google.common.base.Splitter;

public class CopyStep extends Step
{ 
  /** Steps counter. */
  static int number = 0;
  
  /** What ExecuteStep execute _before_ executing this step. */
  private Iterable<String> executeBefore = new ArrayList<>();
  
  /** What ExecuteStep execute _after_ executing this step. */
  private Iterable<String> executeAfter = new ArrayList<>();
  
  public CopyStep()
  {
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
  public static CopyStep create(Element element) throws Exception
  {
    CopyStep result = new CopyStep();
    result.importer = ImporterFactory.newInstance(result);
    result.importer.setPropertiesFromXml(element);
    
    Element exec = XmlUtils.getFirst(element, "exec");
    if (exec != null)
    {
      result.executeBefore = Splitter.on(',').omitEmptyStrings().trimResults().split(exec.getAttribute("before"));
      result.executeAfter = Splitter.on(',').omitEmptyStrings().trimResults().split(exec.getAttribute("after"));
    }
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy importer=%s", importer);
  }
}
