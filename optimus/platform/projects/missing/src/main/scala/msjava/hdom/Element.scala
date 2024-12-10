package msjava.hdom

import java.{util => ju}

class Element(name: String) {
  def getName: String = ???
  def getText: String = ???
  def getChildren(name: String): ju.List[Element] = ???
  def getChildren(): ju.List[Element] = ???
  def getAttributes: ju.List[Attribute] = ???
  def getChildText(name: String): String = ???
  def getValueAsString: String = ???
  def getAttributeValue(name: String): String = ???
  def getAttribute(name: String): Attribute = ???
}
