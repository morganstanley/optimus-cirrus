package msjava.zkapi.internal

import java.io.Closeable
import org.apache.curator.framework.CuratorFramework

class ZkaContext(x: Any) extends Closeable {
  def getRootNode: String = ???
  def getData(p: String): ZkaData = ???
  def getNodeData(mode: String): Array[Byte] = ???
  def getCurator: CuratorFramework = ???
  def close(): Unit = ???
}

object ZkaContext {
  def contextForSubPath(context: ZkaContext, p: String): ZkaContext = ???
}