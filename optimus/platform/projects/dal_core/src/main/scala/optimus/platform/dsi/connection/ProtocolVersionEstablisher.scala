/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.dsi.connection

import java.nio.ByteBuffer
import msjava.msnet._
import msjava.slf4jutils.scalalog.getLogger
import scala.jdk.CollectionConverters._
import optimus.graph.DiagnosticSettings._

/**
 * The establisher is intended to provide a mutually-compatible connection protocol between client and server (see
 * DalProtocolVersion)
 *
 * This is a suitable place to manage breaking changes in the wire format but where possible changes to wire format
 * should be backwards-compatible. Non-breaking wire format changes should not require new protocol versions or changes
 * to this establisher.
 *
 * The wire format for this establisher is a message of the form "DalProtocolVersion: XXXX\n". There are four bytes
 * available for use here. Currently we simply send an integer indicating what the protocol version is; the possible
 * values are the tag values in subtypes of DalProtocolVersion.
 *
 * NB in future this can become non-optional on the client once all brokers are using the establisher factory
 *
 * ***** Preserve backwards compatibility of this establisher at all costs! *****
 */
class DalProtocolVersionEstablisher private (myProtocolVersion: DalProtocolVersion, isOptional: Boolean)
    extends MSNetEstablisher(DalProtocolVersionEstablisher.Priority, DalProtocolVersionEstablisher.TimeoutSecs) {
  import DalProtocolVersionEstablisher._

  setOptional(isOptional)

  private var remoteDalProtocolVersion: DalProtocolVersion = DalProtocolVersion.EstablisherNotUsed

  private var status: MSNetEstablishStatus = MSNetEstablishStatus.UNKNOWN
  private var conn: MSNetTCPConnection = _
  private val writeBuf = new MSNetTCPSocketBuffer

  /* override */ def init(isServerSide: Boolean, conn: MSNetTCPConnection): Unit = {
    require(this.conn eq null, s"Cannot reuse establisher with connection ${this.conn} for new connection $conn")
    log.trace(s"Initializing establisher for ${conn.getAddress}")
    this.conn = conn
  }

  /* override */ def cleanup(): Unit = {
    log.trace(s"Clear write buffer for $conn")
    writeBuf.clear()
  }

  /* override */ def establish(readBuf: MSNetTCPSocketBuffer): MSNetEstablishStatus = {
    log.debug(s"Begin protocol version establishment for $conn")
    if (status.isUnknown) {
      // Write our own protocol data
      writeBuf.store(createProtocolData())
    }
    status = MSNetEstablishStatus.CONTINUE
    // Read protocol data out of the start of readBuf (if possible)
    parseProtocolData(readBuf) foreach { remoteVersion =>
      remoteDalProtocolVersion = remoteVersion
      status = MSNetEstablishStatus.COMPLETE
    }
    if (status.isComplete) {
      log.info(s"Connection $conn using DAL protocol version $remoteDalProtocolVersion")
    } else if (status.isContinue) {
      log.debug(s"No protocol version yet for $conn")
    } else {
      log.warn(s"Unexpected protocol version establishment status $status on connection $conn")
    }
    status
  }

  /* override */ def getStatus: MSNetEstablishStatus = status

  /* override */ def getOutputBuffer: MSNetTCPSocketBuffer = writeBuf

  /* override */ def getEstablisherName: String = "DalProtocolVersionEstablisher"

  private def createProtocolData(): Array[Byte] = {
    val buff = ByteBuffer.allocate(MessageSize)
    buff.put(Header)
    buff.putInt(myProtocolVersion.tag)
    buff.put(LfByte)
    buff.array()
  }

  private def parseProtocolData(readBuf: MSNetTCPSocketBuffer): Option[DalProtocolVersion] = {
    val data = readBuf.peek()
    // We can only parse out protocol version if the string is at the start of the buffer (we can't clear the buffer's
    // tail end) so the data should be at least (prefix+sizeOf(Int)+1) bytes -- possibly more because there may be other
    // data in the buffer after data which we actually want to read
    val prefixMatches = data.length >= MessageSize && ((0 until Header.length) forall { i =>
      data(i) == Header(i)
    })
    // Check that the string terminates with a newline five bytes after the header ends
    val isValidProtocolVersionLine = prefixMatches && data(Header.length + 4) == LfByte
    // Now we can read the tag out
    if (isValidProtocolVersionLine) {
      val tag = ByteBuffer.wrap(data, Header.length, 4).getInt()
      val protocolVersion =
        if (tag > myProtocolVersion.tag) myProtocolVersion
        else DalProtocolVersion.fromTag(tag)
      // Remove the message from the buffer
      readBuf.processed(MessageSize)
      Some(protocolVersion)
    } else None
  }
}

// NB In future this will become non-optional on the server once all clients are using the establisher
class DalProtocolVersionEstablisherFactory(protocolVersion: DalProtocolVersion) extends MSNetEstablisherFactory {
  val optionalProtoEstablisher =
    getBoolProperty("optimus.platform.dsi.connection.dalProtocolVersionEstablisher.optional", false)

  /* override */ def createEstablisher(): MSNetEstablisher =
    DalProtocolVersionEstablisher(protocolVersion, optional = optionalProtoEstablisher)
}

object DalProtocolVersionEstablisher {
  private val log = getLogger[DalProtocolVersionEstablisher]

  // As it stands we run server and client with BACKCOMPAT primary establisher. What this means on the server-side is
  // that when primary establisher runs it will call establish() on the first non-optional establisher it can find in
  // priority order (this is just how the framework is designed). Since we don't have the protocol version establisher
  // installed on all clients and servers, it has to be optional. The result of that is that the server will always end
  // up running a kerberos authenticator as the first establisher. However, the client does not duplicate this logic:
  // it simply uses the priority values of establishers to order things with no regard for whether they are optional or
  // not. Therefore if the priority value we use is smaller than that of the authenticator, the client will end up
  // running the protocol version establisher first and the ordering will not be consistent between client and server.
  // This value therefore needs to be something larger than MSNetAuthenticator.DEFAULT_ESTABLISH_PRIORITY. Unfortunately
  // that value constant is protected so it can't be accessed from out here. At the time of writing it is set to 100 so
  // I have hard-coded 200 here. In a future MSJava version the DEFAULT_ESTABLISH_PRIORITY constant is going to be made
  // public; at that point we should change this value to be something like DEFAULT_ESTABLISH_PRIORITY + 1 rather than
  // using a hard-coded magic number.
  // SEE: more detailed explanation linked in below JIRA
  // TODO (OPTIMUS-14411): magic number -> value derived from DEFAULT_ESTABLISH_PRIORITY
  val Priority = 200

  private val TimeoutSecs: Int =
    Integer.getInteger("optimus.dsi.server.connection.dalProtocolVersionEstablisherTimeoutSecs", 60)
  private val LfByte = '\n'.toByte
  private val Header = "DalProtocolVersion: ".toCharArray.map(_.toByte)
  private val MessageSize = Header.length + 5 // header, then 4-byte integer, then 1-byte newline

  def apply(
      supportedProtocolVersion: DalProtocolVersion = DefaultProtocolVersion,
      optional: Boolean = false
  ): DalProtocolVersionEstablisher = {
    new DalProtocolVersionEstablisher(supportedProtocolVersion, optional)
  }

  // Return values:
  // None indicates that DalProtocolVersionEstablisher isn't being used on the connection (i.e. one end or the other
  // doesn't support it)
  // Some(x) indicates that the two ends have established that x is the highest commonly supported protocol version
  def getRemoteDalProtocolVersion(conn: MSNetTCPConnection): Option[DalProtocolVersion] = {
    conn.establisherIterator().asScala.collectFirst {
      case e: DalProtocolVersionEstablisher if e.getStatus.isComplete => e.remoteDalProtocolVersion
    }
  }

  def factory(protocolVersion: DalProtocolVersion = DefaultProtocolVersion) =
    new DalProtocolVersionEstablisherFactory(protocolVersion)

  private[optimus] val DefaultProtocolVersion = GpbWithDalRequest
}

sealed abstract class DalProtocolVersion(val tag: Int) {
  def isSince(other: DalProtocolVersion): Boolean = tag >= other.tag
}

// Top-level message is DSIRequestProto/DSIResponseProto
case object GpbWithDsiRequest extends DalProtocolVersion(1)
// Support introduced for top-level message to be a DalRequestProto/DalResponseProto
case object GpbWithDalRequest extends DalProtocolVersion(2)

object DalProtocolVersion {
  // Indicates that the other end of this connection doesn't use this establisher. Equivalent to GpbWithDsiRequest as
  // that was the top-level message format when this establisher was introduced
  val EstablisherNotUsed = GpbWithDsiRequest

  def fromTag(tag: Int) = tag match {
    case GpbWithDsiRequest.tag => GpbWithDsiRequest
    case GpbWithDalRequest.tag => GpbWithDalRequest
  }
}
