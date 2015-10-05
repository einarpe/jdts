package kpp.jtds.core;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public final class XmlUtils
{
  /** Return first child with given name within given element. */
  public static Element getFirst(Element parent, String name)
  {
    NodeList nl = parent.getChildNodes();
    for (int i = 0, l = nl.getLength(); i < l; i++)
    {
      Node curr = nl.item(i);
      if (curr.getNodeType() == Element.ELEMENT_NODE && curr.getNodeName().equals(name))
        return (Element) curr;
    }
    
    return null;
  }
}
