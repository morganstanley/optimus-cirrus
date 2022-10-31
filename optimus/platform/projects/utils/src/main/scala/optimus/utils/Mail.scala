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
package optimus.utils

import java.io.File
import java.util.Properties

import javax.activation.DataHandler
import javax.activation.FileDataSource
import javax.mail.Address
import javax.mail.Message
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeBodyPart
import javax.mail.internet.MimeMessage
import javax.mail.internet.MimeMultipart

object Mail {

  private def toAddress(addresses: Seq[String]): Array[Address] =
    addresses.map(new InternetAddress(_)).toArray[Address]

  def send(
      from: String,
      to: Seq[String],
      cc: Seq[String] = Seq(),
      bcc: Seq[String] = Seq(),
      subject: String,
      body: String,
      replyTo: Option[Seq[String]] = None,
      mimeType: String = "text/html",
      attachments: Seq[File] = Seq())(implicit properties: Properties = System.getProperties): Unit = {

    val session = Session.getInstance(properties)
    val multipart = new MimeMultipart()
    val message = new MimeMessage(session)

    message.setFrom(new InternetAddress(from))
    message.addRecipients(Message.RecipientType.TO, toAddress(to))
    message.addRecipients(Message.RecipientType.CC, toAddress(cc))
    message.addRecipients(Message.RecipientType.BCC, toAddress(bcc))
    replyTo.foreach(replyTo => message.setReplyTo(toAddress(replyTo)))

    attachments.foreach { attachment =>
      val messageBodyPart = new MimeBodyPart()
      messageBodyPart.setDataHandler(new DataHandler(new FileDataSource(attachment)))
      messageBodyPart.setFileName(attachment.getName)
      multipart.addBodyPart(messageBodyPart)
    }

    val messageBodyPart = new MimeBodyPart()
    messageBodyPart.setContent(body, mimeType)
    multipart.addBodyPart(messageBodyPart)

    message.setSubject(subject)
    message.setContent(multipart)

    Transport.send(message)
  }
}
