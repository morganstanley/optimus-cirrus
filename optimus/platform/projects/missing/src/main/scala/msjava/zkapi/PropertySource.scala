package msjava.zkapi

import java.io.Closeable

trait PropertySource extends Closeable {
  def getProperty(p: String): AnyRef
}