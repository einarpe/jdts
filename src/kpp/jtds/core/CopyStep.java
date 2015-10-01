package kpp.jtds.core;

import kpp.jdts.importer.Importer;
import kpp.jdts.importer.MySQLImporter;

import org.w3c.dom.Element;

public class CopyStep extends Step
{ 
  static int number = 0;
  
  public CopyStep(DTS dts)
  {
    super(dts);
    ++number;
  }
  
  private Importer importer;

  @Override
  public void execute() throws Exception
  {
    int rows = importer.prepare();
    if (rows > 0)
      importer.insert();
  }
  
  /**
   * Returns instance of CopyStep class based on XML element.
   * @param element - xml element
   * @return 
   */
  public static CopyStep create(Element element, DTS dts)
  {
    CopyStep result = new CopyStep(dts);
    result.dts = dts;
    result.importer = new MySQLImporter(result);
    result.importer.setPropertiesFromXml(element);
    return result;
  }
  
  public String toString()
  {
    return String.format("Copy importer=%s", importer);
  }
}
