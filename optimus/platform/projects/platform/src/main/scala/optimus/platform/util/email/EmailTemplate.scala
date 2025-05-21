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
package optimus.platform.util.email

import freemarker.template.Configuration
import freemarker.template.DefaultObjectWrapperBuilder
import freemarker.template.Template
import freemarker.template.TemplateExceptionHandler
import optimus.platform.platform.config.StaticConfig
import org.apache.commons.io.IOUtils

import java.io.StringWriter
import java.util.Base64
import javax.mail.Part
import javax.mail.internet.MimeBodyPart
import javax.mail.internet.MimeMultipart
import javax.mail.internet.PreencodedMimeBodyPart
import scala.jdk.CollectionConverters._

class EmailTemplate(template: Template, val inputs: Map[String, Object]) {
  import EmailTemplate._

  // use an explicit convert rather than an implicit one
  def asHTML: String = {
    val writer = new StringWriter
    try {
      template.process(inputs.asJava, writer)
      writer.toString
    } finally {
      writer.close()
    }
  }

  def logoBase64Map: Map[String, String] =
    Map(logo -> logoBase64)
}

object EmailTemplate {
  private val templatesDir: String = StaticConfig.string("emailTemplatesDir")
  private val logo: String = StaticConfig.string("emailLogo")
  private val logoPath: String = s"$templatesDir/$logo"

  private val config = {
    val cfg = new Configuration(Configuration.VERSION_2_3_20)
    cfg.setObjectWrapper(new DefaultObjectWrapperBuilder(Configuration.VERSION_2_3_20).build())
    cfg.setDefaultEncoding("UTF-8")
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER)
    cfg.setClassForTemplateLoading(this.getClass, templatesDir)
    cfg
  }

  private lazy val logoBase64 = {
    val logoInputStream = this.getClass.getResourceAsStream(logoPath)
    try {
      val logoBytes = IOUtils.toByteArray(logoInputStream)
      Base64.getEncoder.encodeToString(logoBytes)
    } finally {
      logoInputStream.close()
    }
  }

  lazy val postReviewHTMLTemplate = config.getTemplate("postreview/HtmlReport.ftl")
  lazy val requestHTMLTemplate = config.getTemplate("request/HtmlRequest.ftl")
  lazy val entitlementRequestHTMLTemplate = config.getTemplate("request/HtmlEntitlementRequest.ftl")
  lazy val entitlementViolationTemplate = config.getTemplate("entitlement/ViolationReport.ftl")
  lazy val entitlementGroupViolationTemplate = config.getTemplate("entitlement/GroupViolationReport.ftl")
  lazy val entitlementReviewTableTemplate = config.getTemplate("entitlement/EntitlementReviewTable.ftl")
  lazy val entitlementAnnouncementTemplate = config.getTemplate(StaticConfig.string("entitlementAnnouncementTemplate"))
  lazy val entitlementReviewOverviewTemplate = config.getTemplate("entitlement/EntitlementReviewOverview.ftl")
  lazy val configReconReportTemplate = config.getTemplate("recon/ConfigReconReport.ftl")
  lazy val upgradeAnnouncement = config.getTemplate("upgrade/UpgradeAnnouncement.ftl")
  lazy val throttlingZoneManagerTemplate = config.getTemplate("zones/ThrottlingZoneManagerAnnouncement.ftl")
  lazy val zonesRequest = config.getTemplate("zones/ZonesRequest.ftl")
  lazy val zonesRequestReview = config.getTemplate("zones/ZonesRequestReview.ftl")
  lazy val zonesRequestApproval = config.getTemplate("zones/ZonesRequestApproval.ftl")
  lazy val throttlingAnnouncementTemplate = config.getTemplate(StaticConfig.string("throttlingAnnouncementTemplate"))
  lazy val shardRequest = config.getTemplate("shards/ShardRequest.ftl")
  lazy val uowConsumerLagAnnouncement = config.getTemplate("uow/UoWConsumerLagAnnouncement.ftl")
  lazy val dalKafkaConsumerLagAnnouncement = config.getTemplate("monitoring/DalKafkaConsumerLagAnnouncement.ftl")
  lazy val keyRotationReminder = config.getTemplate("security/KeyRotationReminder.ftl")
  lazy val simpleTemplate = config.getTemplate("simple.ftl.html")

  def apply(template: Template, inputs: Map[String, Object]): EmailTemplate = new EmailTemplate(template, inputs)

  object BackgroundColors {
    val Green = "#62b902"
    val Blue = "#005791"
    val Amber = "#FFA200"
    val Orange = "#ff6600"
    val Gray = "#747f81"
    val Purple = "#666699"
  }

  def simpleTemplate(title: String, message: String, contactUsEmail: String, headBgColor: String): EmailTemplate = {
    apply(
      simpleTemplate,
      Map(
        "title" -> title,
        "message" -> message,
        "contactUs" -> contactUsEmail,
        "bgColor" -> headBgColor
      ))
  }
}

object EmailTemplateHelper {
  def createHTMLMimeMultipart(emailTemplate: EmailTemplate): MimeMultipart = {
    createHTMLMimeMultipart(emailTemplate, None, Set.empty[String])
  }

  def createHTMLMimeMultipart(
      emailTemplate: EmailTemplate,
      textContentOpt: Option[String],
      attachments: Set[String] = Set.empty[String],
      includeLogo: Boolean = true): MimeMultipart = {
    // emailWrapper wraps all including html/text and ms_logo
    val emailWrapper = new MimeMultipart("related")

    // htmlOrTextBodyPart use an alternative way to display html(or text when client does not support html)
    val htmlOrTextBodyPart = new MimeBodyPart()
    val htmlOrTextMultipart = new MimeMultipart("alternative")
    textContentOpt.foreach { textContent =>
      htmlOrTextMultipart.addBodyPart({
        val textBodyPart = new MimeBodyPart()
        textBodyPart.setText(textContent, "utf-8")
        textBodyPart
      })
    }
    htmlOrTextMultipart.addBodyPart({
      val htmlBodyPart = new MimeBodyPart()
      htmlBodyPart.setContent(emailTemplate.asHTML, "text/html; charset=utf-8")
      htmlBodyPart
    })
    htmlOrTextBodyPart.setContent(htmlOrTextMultipart)
    emailWrapper.addBodyPart(htmlOrTextBodyPart)

    // add image as content-id
    if (includeLogo)
      emailTemplate.logoBase64Map.foreach { case (imgName, imgBase64) =>
        val imgBodyPart = new PreencodedMimeBodyPart("base64")
        imgBodyPart.setFileName(imgName)
        imgBodyPart.setHeader("Content-ID", s"<$imgName>")
        imgBodyPart.setDisposition(Part.INLINE)
        imgBodyPart.setText(imgBase64)
        emailWrapper.addBodyPart(imgBodyPart)
      }

    // add attachments
    attachments.foreach { fileLocation =>
      val attachPart = new MimeBodyPart()
      attachPart.attachFile(fileLocation)
      htmlOrTextMultipart.addBodyPart(attachPart)
    }
    emailWrapper
  }
}
