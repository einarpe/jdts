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

  public static void init(String xmlFilePath) throws Exception
  {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    document = db.parse(new File(xmlFilePath));
    xp = XPathFactory.newInstance();
  }
  
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

  public static Document getXmlDocument()
  {
    return document;
  }
  
  
}
