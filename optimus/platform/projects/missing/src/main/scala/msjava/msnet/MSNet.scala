package msjava.msnet

import java.{util => ju}
import java.net.Socket

class MSNetEstablisher(p: Int, t: Int) {
  def setOptional(b: Boolean): Unit = ???
}
class MSNetEstablisherFactory

class MSNetTCPSocketBuffer {
  def processed(MessageSize: Int): Unit = ???
  def peek(): Array[Byte] = ???
  def store(bytes: Array[Byte]): Unit = ???
  def clear(): Unit = ???
}
class MSNetTCPConnection {
  def getAddress: String = ???
  def establisherIterator(): ju.Iterator[MSNetEstablisher] = ???
  def setImmutable(b: Boolean): Unit = ???
  def addListener(b: Any): Unit = ???
  def setMasterEstablisherMode(b: Any): Unit = ???
  def asyncSend(msg: Any): Unit = ???
}
trait MSNetEstablishStatus {
  def isUnknown: Boolean = ???
  def isContinue: Boolean = ???
  def isComplete: Boolean = ???
}
object MSNetEstablishStatus {
  object CONTINUE extends MSNetEstablishStatus
  object UNKNOWN extends MSNetEstablishStatus
  object COMPLETE extends MSNetEstablishStatus
}
class MSNetInetAddress {
  def getImpl(): MSNetInetAddressImpl = ???
}
trait MSNetInetAddressImpl {
  def getHost(): String = ???
  def getPort(): Int = ???
}
object AbstractEstablisherHandler {
  def kerberosAuth(a: Socket, b: Boolean, c: Int): String = ???
}
class MSNetEstablishException extends Exception
class MSNetMessage
trait MSNetConnectionListenerAdapter {
  def readCallback(name: MSNetID, message: MSNetMessage): Unit
  def disconnectCallback(name: MSNetID): Unit
  def connectCallback(name: MSNetID): Unit
}
class MSNetID
class MSNetProtocolTCPConnection(config: internal.MSNetProtocolConnectionConfigurationSupport) extends MSNetTCPConnection
class MSNetExecption(cause: String) extends Exception(cause)
