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
package optimus.platform.appgroupinfo

import com.opencsv._
import optimus.platform.IO.usingQuietly
import optimus.platform.installcommonpath.MetaProjectRelease

import java.io._
import scala.jdk.CollectionConverters._

object AppGroupFinder {
  val notSetPlaceHolder: String = "NotSet"

  def findAppGroupFromFilePath(appGroupFilePath: String, mpr: MetaProjectRelease): Option[String] =
    findAppGroupFromInputStream(new FileInputStream(appGroupFilePath), mpr)

  def findAppGroupFromInputStream(appGroupInputStream: InputStream, mpr: MetaProjectRelease): Option[String] = {
    appGroupsFromInputStream(appGroupInputStream)
      .collectFirst {
        case groupInfo: AppGroupInfo
            if groupInfo.meta == mpr.meta && groupInfo.project == mpr.proj && mpr.release.contains(
              groupInfo.releaseLink) =>
          groupInfo.appGroup
      }
  }

  def appGroupsFromFilePath(appGroupFilePath: String): Seq[AppGroupInfo] = appGroupsFromInputStream(
    new FileInputStream(appGroupFilePath))

  def appGroupsFromInputStream(appGroupInputStream: InputStream): Seq[AppGroupInfo] = {
    usingQuietly(new CSVReader(new InputStreamReader(appGroupInputStream))) { reader =>
      val lines = reader.readAll().asScalaUnsafeImmutable.map(_.toSeq)
      val maybeHeader = lines.headOption
      maybeHeader match {
        case Some(header) =>
          val trimmedHeader = header.map(_.trim)
          val columnInfo = AppGroupInfoColumnIndexes(
            trimmedHeader.indexOf(AppGroupInfoColumn.AppGroup.toString),
            trimmedHeader.indexOf(AppGroupInfoColumn.Meta.toString),
            trimmedHeader.indexOf(AppGroupInfoColumn.Project.toString),
            trimmedHeader.indexOf(AppGroupInfoColumn.ReleaseLink.toString),
            trimmedHeader.indexOf(AppGroupInfoColumn.OwnerGroup.toString),
            trimmedHeader.indexOf(AppGroupInfoColumn.ModelCode.toString)
          )

          if (columnInfo.isValid) {
            lines.tail.map { l =>
              val appGroup = getAndTrim(l, columnInfo.appGroupIndex)
              val meta = getAndTrim(l, columnInfo.metaIndex)
              val project = getAndTrim(l, columnInfo.projectIndex)
              val releaseLink = getAndTrim(l, columnInfo.releaseLinkIndex)
              val group = getAndTrim(l, columnInfo.groupIndex)
              val modelCode = getAndTrim(l, columnInfo.modelCodeIndex)

              AppGroupInfo(appGroup, meta, project, releaseLink, group, AppGroupInfoColumn.modelCodePresent(modelCode))
            }
          } else
            Seq.empty
        case None => Seq.empty
      }
    }
  }

  private def getAndTrim(line: Seq[String], index: Int): String = line(index).trim
}

final case class AppGroupInfo(
    appGroup: String,
    meta: String,
    project: String,
    releaseLink: String,
    ownerGroup: String,
    modelCode: Boolean)

object AppGroupInfoColumn extends Enumeration {
  type AppGroupInfoColumn = Value

  val AppGroup: AppGroupInfoColumn = Value("AppGroup")
  val Meta: AppGroupInfoColumn = Value("Meta")
  val Project: AppGroupInfoColumn = Value("Project")
  val ReleaseLink: AppGroupInfoColumn = Value("ReleaseLink")
  val OwnerGroup: AppGroupInfoColumn = Value("OwnerGroup")
  val ModelCode: AppGroupInfoColumn = Value("ModelCode")

  private val modelCodePresent: String = "Y"

  def modelCodePresent(modelCodeColumn: String): Boolean = modelCodeColumn == modelCodePresent
}

final case class AppGroupInfoColumnIndexes(
    appGroupIndex: Int,
    metaIndex: Int,
    projectIndex: Int,
    releaseLinkIndex: Int,
    groupIndex: Int,
    modelCodeIndex: Int) {
  def isValid: Boolean =
    appGroupIndex >= 0 && metaIndex >= 0 && projectIndex >= 0 && releaseLinkIndex >= 0 && groupIndex >= 0 && modelCodeIndex >= 0
}
