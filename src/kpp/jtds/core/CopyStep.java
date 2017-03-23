package kpp.jtds.core;

import java.util.Collections;
import java.util.List;

import kpp.jdts.importer.Importer;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class CopyStep extends Step
{ 
  /** Steps counter. */
  static int number = 0;
  
  /** What ExecuteStep execute _before_ executing this step. */
  private List<String> executeBefore = Collections.emptyList();
  
  /** What ExecuteStep execute _after_ executing this step. */
  private List<String> executeAfter = Collections.emptyList();

  private String into;

  private String query;

  private boolean truncate;

  private String behaviour;
  
  public CopyStep()
  {
    ++number;
  }
  
  @Override
  public void execute(DTS dts) throws Exception
  {
    dts.executeSteps(executeBefore); 
    
    Importer importer = dts.getImporter(this);
    int rows = importer.prepare();
    if (rows > 0)
      importer.insert();
    
    dts.executeSteps(executeAfter); 
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
    Element exec = XmlUtils.getFirst(element, "exec");
    if (exec != null)
    {
      result.executeBefore = Lists.newArrayList(Splitter.on(',').omitEmptyStrings().trimResults().split(exec.getAttribute("before")));
      result.executeAfter = Lists.newArrayList(Splitter.on(',').omitEmptyStrings().trimResults().split(exec.getAttribute("after")));
    }
    
    NodeList queries = element.getElementsByTagName("query");
    if (queries.getLength() > 0)
      result.query = ((Element)queries.item(0)).getTextContent();
    
    result.into = element.getAttribute("into");
    result.truncate = element.getAttribute("truncate").equalsIgnoreCase("true");
    result.behaviour = element.getAttribute("behaviour");
    
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy");
  }

  public String getInto()
  {
    return into;
  }

  public String getQuery()
  {
    return query;
  }

  public boolean getTruncate()
  {
    return truncate;
  }

  public String getBehaviour()
  {
    return behaviour;
  }
}
