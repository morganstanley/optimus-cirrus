package msjava.zkapi.internal

class ZkaData {
  def get(x: String): AnyRef = ???
  def getRawMap(): Nothing = ???
}

object ZkaData {
  def fromBytes(bytes: Array[Byte]): ZkaData = ???
}