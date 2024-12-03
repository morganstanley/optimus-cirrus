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
package optimus.stratosphere.utils

import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.InetAddress
import java.time.ZonedDateTime
import java.util.Properties
import javax.activation.CommandMap
import javax.activation.MailcapCommandMap
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

object Mail {

  def sendErrorMessage(from: String, to: String, subject: String, smtpHost: String, exception: Throwable): Unit = {
    val writer = new StringWriter()
    exception.printStackTrace(new PrintWriter(writer))

    send(
      from = from,
      to = Seq(to),
      cc = Seq(from),
      subject = s"[${ZonedDateTime.now().toLocalDate.toString}] $subject.",
      body = s"""
                |<b>$subject:</b><br><br>
                |
                |    <div style="margin-left: 50px;">
                |        user: ${System.getProperty("user.name")}<br>
                |        host: ${InetAddress.getLocalHost.getHostName}<br>
                |        exception: ${exception.getClass.getSimpleName}:<br><br>
                |
                |        <div style="margin-left: 50px;">
                |          <pre>
                |${exception.getMessage}
                |$writer
                |          </pre>
                |        </div>
                |    </div>
                |""".stripMargin,
      smtpHost = smtpHost
    )
  }

  def send(
      from: String,
      to: Seq[String],
      cc: Seq[String] = Seq(),
      bcc: Seq[String] = Seq(),
      subject: String,
      body: String,
      smtpHost: String,
      mimeType: String = "text/html",
      attachments: Seq[File] = Seq()): Unit =
    try {
      implicit val properties: Properties = System.getProperties
      properties.setProperty("mail.smtp.host", smtpHost)
      properties.setProperty("mail.transport.protocol", "smtp")

      // Without the MailcapCommandMap Mail.send(..) fails silently because internally an exception flies:
      // javax.mail.MessagingException: IOException while sending message;
      //  nested exception is:
      //	javax.activation.UnsupportedDataTypeException: no object DCH for MIME type multipart/mixed;
      //	boundary="----=_Part_0_1517325819.1674550636789"
      val mc = CommandMap.getDefaultCommandMap().asInstanceOf[MailcapCommandMap];
      mc.addMailcap("multipart/*;; x-java-content-handler=com.sun.mail.handlers.multipart_mixed");

      optimus.utils.Mail.send(from, to, cc, bcc, subject, body, mimeType = mimeType, attachments = attachments)
    } catch {
      case NonFatal(e) => println(s"[ERROR] Sending email failed due to: $e!")
    }
}
