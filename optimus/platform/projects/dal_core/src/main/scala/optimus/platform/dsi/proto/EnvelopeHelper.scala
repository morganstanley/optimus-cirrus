/* /*
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
package optimus.platform.dsi.proto

import com.google.protobuf.CodedInputStream
import com.google.protobuf.WireFormat
import optimus.platform.dsi.bitemporal.proto.Envelope

import scala.annotation.tailrec

object EnvelopeHelper {
  def extractHeader(bytes: Array[Byte]): Option[Envelope.DalRequestResponseHeader] = {
    val stream = CodedInputStream.newInstance(bytes)
    @tailrec
    def parseHeader(): Option[Envelope.DalRequestResponseHeader] = {
      val tag = stream.readTag()
      val fieldNumber = WireFormat.getTagFieldNumber(tag)
      if (fieldNumber == Envelope.DalRequestResponseEnvelope.HEADER_FIELD_NUMBER) {
        val hdrsz = stream.readRawVarint32()
        val ol = stream.pushLimit(hdrsz)
        val header = Envelope.DalRequestResponseHeader.parseFrom(stream)
        stream.popLimit(ol)
        Some(header)
      } else if (fieldNumber < Envelope.DalRequestResponseEnvelope.HEADER_FIELD_NUMBER) {
        stream.skipField(tag)
        parseHeader()
      } else None
    }

    parseHeader()
  }
}
 */