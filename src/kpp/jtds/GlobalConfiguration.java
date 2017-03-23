package kpp.jtds;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import kpp.jdts.csv.dialect.Dialect;
import kpp.jdts.csv.dialect.Dialects;
import kpp.jtds.core.ConnectionData;
import kpp.jtds.core.Logger;
import kpp.jtds.core.Step;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class GlobalConfiguration
{
  private static Document document;
  
  private static XPathFactory xp;

  /** Init configuration with XML file. */
  public static void init(String xmlFilePath) throws Exception
  {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    document = db.parse(new File(xmlFilePath));
    xp = XPathFactory.newInstance();
  }
  
  /** Returns XML element defining importer to use. 
   * @throws Exception */
  public static Element getImporterElement()
  {
    try
    {
      Element importerData = (Element)xp.newXPath().evaluate("/dts/config/importer", document.getDocumentElement(), XPathConstants.NODE);
      if (importerData == null)
        throw new Exception("Importer config data not found!");
      
      return importerData;
    }
    catch (Exception e)
    {
      Logger.error("Importer definition not found. ", e.getMessage());
      return null;
    }
  }
  
  /** Returns linked list of steps to perform. */
  public static List<Step> getSteps()
  {
    List<Step> ret = new LinkedList<Step>();
    try
    {
      NodeList steps = (NodeList)xp.newXPath().evaluate("/dts/steps/*", document.getDocumentElement(), XPathConstants.NODESET);
      for (int i = 0, l = steps.getLength(); i < l; i++)
      {
        Element step = (Element) steps.item(i);
        Step stepObject = Step.create(step);
        ret.add(stepObject);
      }
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Steps definitions not found! ", e.getMessage());
    }
    catch (Exception e)
    {
      Logger.error("Error loading steps! ", e.getMessage());
    }
    return ret;
  }
  
  /** Default buffer for FileStringBuilder. */
  final static int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;
  
  /** Returns integer defining buffer size. */
  public static int getBufferSize()
  {
    try
    {
      Element buffer = (Element)xp.newXPath().evaluate("/dts/config/buffer", document.getDocumentElement(), XPathConstants.NODE);
      if (buffer == null)
        return DEFAULT_BUFFER_SIZE;
      
      String buffSizeStr = buffer.getAttribute("size");
      return Integer.parseInt(buffSizeStr);
    }
    catch (XPathExpressionException e)
    {
      return DEFAULT_BUFFER_SIZE; 
    }
  }
  
  /** Configuration of temporary files */
  public static TempFileConfig getTempFileConfig()
  {
    TempFileConfig ret = new TempFileConfig();
    ret.Dir = System.getProperty("java.io.tmpdir");
    ret.KeepFiles = false;
    ret.Pattern = "jdts[NOW].csv";
    
    try
    {
      Element tempfiles = (Element)xp.newXPath().evaluate("/dts/config/tempfiles", document.getDocumentElement(), XPathConstants.NODE);
      if (tempfiles != null)
      {
        String dir = tempfiles.getAttribute("dir");
        if (dir != null && !dir.trim().isEmpty())
          ret.Dir = dir;
        
        ret.KeepFiles = Boolean.parseBoolean(tempfiles.getAttribute("keep"));
      }
    }
    catch (XPathExpressionException e)
    {
    }
    return ret;
  }
  
  /** Get dialect instance from csv element. When result is null then dialect was not defined in xml file or was defined wrongly. */
  public static DialectConfig getCSVDialect()
  {
    DialectConfig ret = new DialectConfig();
    try
    {
      Element csv = (Element)xp.newXPath().evaluate("/dts/config/csv", document.getDocumentElement(), XPathConstants.NODE);
      if (csv != null)
      {
        String dialect = csv.getAttribute("dialect");
        if (!dialect.isEmpty())
        {
          ret.UsingDialect = (Dialect) Dialects.class.getDeclaredField(dialect).get(null);
          Logger.debug("Using CSV dialect ", ret.getClass().getName());
        }
      }
    }
    catch (Exception e)
    {
      Logger.error(e.getMessage());
    }
    return ret;
  }
  
  public static class DialectConfig
  {
    public Dialect UsingDialect;
    public String QuoteCharacter;
    public String Delimiter;
    
  }
  
  /** Get instance of Document interface of XML file passed to program as argument. */
  public static Document getXmlDocument()
  {
    return document;
  }
  
  /** Class containing values for handling temporary files. */
  public static final class TempFileConfig
  {
    /** Directory where tempfiles should be saved into. */
    public String Dir;
    
    /** Keep temporary files after jdts ends it's job? */
    public boolean KeepFiles;

    /** Pattern of filename. */
    public String Pattern;
  }
  
  /** Returns XML element defining connection to source database. */
  public static ConnectionData getSourceConnection()
  {
    try
    {
      Element ele = (Element)xp.newXPath().evaluate("/dts/connections/source", document.getDocumentElement(), XPathConstants.NODE);
      return ConnectionData.fromXml(ele);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Source connection definition not found. ", e.getMessage());
      return null;
    }
    catch (ClassNotFoundException e)
    {
      Logger.error("Driver class not found. ", e.getMessage());
      return null;
    }
  }
  
  /** Returns XML element defining connection to destination database. */
  public static ConnectionData getDestinationConnection()
  {
    try
    {
      Element ele = (Element)xp.newXPath().evaluate("/dts/connections/destination", document.getDocumentElement(), XPathConstants.NODE);
      return ConnectionData.fromXml(ele);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Destination definition not found. ", e.getMessage());
      return null;
    }
    catch (ClassNotFoundException e)
    {
      Logger.error("Driver class not found. ", e.getMessage());
      return null;
    }
  }
  
}
