package msjava.base.util.uuid

import java.util.UUID

class MSUuid(x: Array[Byte], y: Boolean) {
  def this(x: String) = this(???, ???)
  def this() = this(???, ???)

  def asBytes(): Array[Byte] = ???
  def asString(): String = ???
  def isTrueBase64(): Boolean = ???
}

object MSUuid {
  def generateJavaUUID(): UUID = ???
}