package kpp.jtds;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import kpp.jtds.core.Logger;

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
  
  /** Returns XML element defining importer to use. */
  public static Element getImporterElement()
  {
    try
    {
      return (Element)xp.newXPath().evaluate("/dts/config/importer", document.getDocumentElement(), XPathConstants.NODE);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Importer definition not found. ", e.getMessage());
      return null;
    }
  }
  
  /** Returns XML list defining list of steps to perform. */
  public static NodeList getSteps()
  {
    try
    {
      return (NodeList)xp.newXPath().evaluate("/dts/steps/*", document.getDocumentElement(), XPathConstants.NODESET);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Steps definitions not found. ", e.getMessage());
      return null;
    }
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
  public static Element getSourceConnection()
  {
    try
    {
      return (Element)xp.newXPath().evaluate("/dts/connections/source", document.getDocumentElement(), XPathConstants.NODE);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Source connection definition not found. ", e.getMessage());
      return null;
    }
  }
  
  /** Returns XML element defining connection to destination database. */
  public static Element getDestinationConnection()
  {
    try
    {
      return (Element)xp.newXPath().evaluate("/dts/connections/destination", document.getDocumentElement(), XPathConstants.NODE);
    }
    catch (XPathExpressionException e)
    {
      Logger.error("Destination definition not found. ", e.getMessage());
      return null;
    }
  }
  
}
