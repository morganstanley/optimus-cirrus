package msjava.zkapi.internal

import msjava.zkapi.PropertySource

class ZkaPropertySource(x: String, y: ZkaContext) extends PropertySource {
  def getProperty(p: String): AnyRef = ???
  def close(): Unit = ???
}
