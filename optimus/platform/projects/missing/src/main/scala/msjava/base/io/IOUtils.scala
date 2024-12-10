package msjava.base.io

import java.io.InputStream
import java.io.OutputStream

object IOUtils {
  def readBytes(stream: InputStream): Array[Byte] = ???

  def copy(input: InputStream, output: OutputStream, size: Int): Unit = ()
}
