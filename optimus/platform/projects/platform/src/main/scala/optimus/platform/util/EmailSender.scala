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
package optimus.platform.util

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.crumbs.{Properties => CrumbProperties}
import optimus.config.RuntimeConfiguration
import optimus.core.ChainedNodeID
import optimus.graph.DiagnosticSettings
import optimus.platform.EvaluationContext
import optimus.platform.platform.config.StaticConfig

import java.util.Date
import java.util.Properties
import javax.mail._
import javax.mail.internet._
import scala.util.control.NonFatal

object EmailSender {
  private val log = getLogger(this)
  def default: EmailSender = EmailSender()

  // constructors are overcomplicated here in order to keep it consistent with EmailSender until they're merged
  def apply(): EmailSender = new EmailSender()
  def apply(smtp: EmailSenderSmtpServer): EmailSender = new EmailSender(Some(smtp))
}

class EmailSender private (
    requestedSmtp: Option[EmailSenderSmtpServer]
) {

  def this() = this(None)

  val smtp: EmailSenderSmtpServer = EmailSenderHelper.resolveSmtpHost(requestedSmtp)

  import EmailSender.log

  private lazy val defaultSender: String = System.getProperty("user.name")

  def send(
      subject: String,
      content: String,
      destination: Set[String],
      from: Option[String] = None,
      replyTo: Option[Set[String]] = None,
      messageType: Option[String] = None,
      cc: Option[Set[String]] = None
  ): Unit = {
    val realFrom = from.getOrElse(defaultSender)
    val message = createMessage(subject, destination, realFrom, replyTo, cc)
    if (messageType.isDefined) { message.setContent(content, messageType.get) }
    else { message.setText(content) }

    doSend(subject, destination, realFrom, message)
  }

  def sendFile(
      subject: String,
      content: String,
      destination: Set[String],
      filePathList: Set[String],
      from: Option[String] = None,
      replyTo: Option[Set[String]] = None,
      cc: Option[Set[String]] = None,
      messageType: Option[String] = None
  ): Unit = {
    val realFrom = from.getOrElse(defaultSender)
    val message = createMessage(subject, destination, realFrom, replyTo, cc)
    val multipart = new MimeMultipart()
    filePathList.foreach { file =>
      val fileBodyPart = new MimeBodyPart()
      fileBodyPart.attachFile(file)
      multipart.addBodyPart(fileBodyPart)
    }
    val messageBodyPart = new MimeBodyPart()
    if (messageType.isDefined) { messageBodyPart.setContent(content, messageType.get) }
    else { messageBodyPart.setText(content) }
    multipart.addBodyPart(messageBodyPart)
    message.setContent(multipart)
    doSend(subject, destination, realFrom, message)
  }

  def sendHTML(
      subject: String,
      htmlContent: MimeMultipart,
      destination: Set[String],
      from: Option[String] = None,
      replyTo: Option[Set[String]] = None,
      cc: Option[Set[String]] = None
  ): Unit = {
    val realFrom = from.getOrElse(defaultSender)
    val message = createMessage(subject, destination, realFrom, replyTo, cc)
    message.setContent(htmlContent)
    doSend(subject, destination, realFrom, message)
  }

  protected def doSend(subject: String, destination: Set[String], realFrom: String, msg: Message): Unit = {
    log.info("Sending email from '{}' to '{}' with subject '{}'", realFrom, destination.mkString(","), subject)
    Transport.send(msg)
    log.info("Email sent")
    EmailSenderHelper.sendCrumb(s"${getClass.getName}.doSend", realFrom, destination)
  }

  protected def createMessage(
      subject: String,
      destination: Set[String],
      from: String,
      replyTo: Option[Set[String]],
      cc: Option[Set[String]]): Message = {
    val props: Properties = new Properties()
    props.put("mail.smtp.host", smtp.host)
    props.put("mail.smtp.port", smtp.port.toString)
    val session: Session = Session.getInstance(props)
    val msg: Message = new MimeMessage(session)
    msg.setHeader("X-Unsent: 1", "")
    msg.setFrom(new InternetAddress(from))
    val address = destination.map(p => new InternetAddress(p))
    msg.setRecipients(Message.RecipientType.TO, address.toArray)
    msg.setSentDate(new Date())
    msg.setSubject(subject)
    replyTo match {
      case Some(addresses) =>
        msg.setReplyTo(addresses.map(p => new InternetAddress(p)).toArray)

      case None =>
    }
    cc.foreach(addresses => {
      val address = addresses.map(f => new InternetAddress(f))
      msg.setRecipients(Message.RecipientType.CC, address.toArray)
    })

    msg
  }
}

object EmailSenderHelper {
  def sendCrumb(
      api: String,
      sender: String,
      recipients: Set[String],
      additionalProperties: Seq[CrumbProperties.Elem[_]] = Seq.empty
  ): Unit = {
    if (Breadcrumbs.isInfoEnabled) {
      val mainProperties: Seq[CrumbProperties.Elem[_]] = Seq(
        CrumbProperties.Elem(CrumbProperties.senderAPI, api),
        CrumbProperties.Elem(CrumbProperties.sender, sender),
        CrumbProperties.Elem(CrumbProperties.recipientDomains, recipients.map { _.split("@").last }.toSeq)
      )

      Breadcrumbs.info(
        ChainedNodeID.nodeID,
        id => PropertiesCrumb(id, EmailSenderSource, mainProperties ++ additionalProperties: _*)
      )
    }
  }

  object EmailSenderSource extends Crumb.Source {
    override val name = "EmailSender"
  }

  def resolveSmtpHost(requested: Option[EmailSenderSmtpServer]): EmailSenderSmtpServer = {

    val (nonObviousEnv, env) =
      try {
        (false, EvaluationContext.env.config.runtimeConfig.env)
      } catch {
        case NonFatal(_) => (true, "")
      }

    val isProd =
      !EvaluationContext.isInitialised || nonObviousEnv || RuntimeConfiguration.isProdEnv(env)

    val allowProdSmtp = DiagnosticSettings.getBoolProperty("EmailSender.allowProdSmtp", false)

    val resolvedRequested = requested.getOrElse(EmailSenderSmtpServer.Prod)
    if (resolvedRequested == EmailSenderSmtpServer.Prod && !isProd && !allowProdSmtp) EmailSenderSmtpServer.Qa
    else resolvedRequested
  }
}

sealed trait EmailSenderSmtpServer {
  def host: String
  def port: Int = 25
  final def hostPort: String = s"$host:$port"
}
object EmailSenderSmtpServer extends Enum[EmailSenderSmtpServer] {
  case object Prod extends EmailSenderSmtpServer {
    override def host = StaticConfig.string("prodSmtpHost")
  }
  case object Qa extends EmailSenderSmtpServer {
    override def host = StaticConfig.string("qaSmtpHost")
  }
  case object LocalTest extends EmailSenderSmtpServer {
    override def host = "localhost"
    override def port = 25000
  }
}
