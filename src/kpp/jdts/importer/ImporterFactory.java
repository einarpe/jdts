package kpp.jdts.importer;

import kpp.jtds.GlobalConfiguration;
import kpp.jtds.core.CopyStep;
import kpp.jtds.core.DTS;
import kpp.jtds.core.Logger;
import kpp.jtds.core.Step;

import org.w3c.dom.Element;

public class ImporterFactory
{
  
  static Importer instance;
  
  /**
   * Create instance of importer basing on configuration file.
   * @param dts 
   * @param step - step
   * @return instance based on class defined in config file
   * @throws Exception when something will go wrong
   */
  public static Importer newInstance(DTS dts, CopyStep step) throws Exception
  {
    Element importerEl = GlobalConfiguration.getImporterElement();
    String className = importerEl.getAttribute("class");
    
    Logger.debug("Creating importer instance of class ", className);
    
    Class<?> cls = Class.forName(className);
    
    Importer imp = (Importer)cls.getDeclaredConstructor(DTS.class, Step.class).newInstance(dts, step);
    return imp;
  }
  
}
