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
package optimus.buildtool.resolvers

import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object DependencyDownloadTracker {
  private val slowestFilesPrintNum = 10
  private val _downloadedHttpFiles: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala
  private val _brokenFiles: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala
  private val _brokenMetadata: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala
  private val _failedMetadata: mutable.Set[String] = ConcurrentHashMap.newKeySet[String]().asScala
  private val _downloadDuration: mutable.Set[(String, Long)] = ConcurrentHashMap.newKeySet[(String, Long)]().asScala
  private val _failedDownloadDuration: mutable.Set[(String, Long)] =
    ConcurrentHashMap.newKeySet[(String, Long)]().asScala
  private val _fetchDuration: mutable.Set[(String, Long)] = ConcurrentHashMap.newKeySet[(String, Long)]().asScala

  // setters
  def clean(): Unit = {
    _downloadedHttpFiles.clear()
    _brokenFiles.clear()
    _brokenMetadata.clear()
    _failedMetadata.clear()
    _downloadDuration.clear()
    _failedDownloadDuration.clear()
    _fetchDuration.clear()
  }
  def addHttpFile(url: String): Boolean = {
    ObtTrace.addToStat(ObtStats.MavenDownloads, 1)
    _downloadedHttpFiles.add(url)
  }
  def addBrokenFile(path: String): Boolean = {
    ObtTrace.addToStat(ObtStats.MavenCorruptedJar, 1)
    _brokenFiles.add(path)
  }
  def addBrokenMetadata(name: String): Boolean = {
    ObtTrace.addToStat(ObtStats.CoursierCorruptedMetadata, 1)
    _brokenMetadata.add(name)
  }
  def addFailedMetadata(name: String): Boolean = {
    ObtTrace.addToStat(ObtStats.CoursierFetchFailure, 1)
    _failedMetadata.add(name)
  }
  def addDownloadDuration(url: String, duration: Long): Boolean = _downloadDuration.add(url, duration)
  def addFailedDownloadDuration(url: String, duration: Long): Boolean = _failedDownloadDuration.add(url, duration)
  def addFetchDuration(name: String, duration: Long): Boolean = _fetchDuration.add(name, duration)

  // getters
  def downloadedHttpFiles: Set[String] = _downloadedHttpFiles.toSet
  def brokenFiles: Set[String] = _brokenFiles.toSet
  def brokenMetadata: Set[String] = _brokenMetadata.toSet
  def failedMetadata: Set[String] = _failedMetadata.toSet
  def downloadDuration: Map[String, Long] = _downloadDuration.toMap
  def failedDownloadDuration: Map[String, Long] = _failedDownloadDuration.toMap
  def fetchDuration: Map[String, Long] = _fetchDuration.toMap

  private def timeStr(input: Long) = Utils.durationString(input / 1000000L)

  private def getSlowestFilesMsgs(files: Map[String, Long], msgKey: String): Seq[String] = if (files.nonEmpty) {
    val displayNum = if (files.size > slowestFilesPrintNum) slowestFilesPrintNum else files.size
    var index = 0
    files.toSeq
      .sortBy(-_._2)
      .take(displayNum)
      .map { case (f, t) =>
        index += 1
        s"($index) $msgKey '$f' in ${timeStr(t)}"
      }
  } else Nil

  // remote dependency download summary msg
  def summary: Seq[String] = {
    val depDownloadMsgKey = if (downloadedHttpFiles.isEmpty) "loaded from local disk" else "downloaded"
    val slowestDownloads = getSlowestFilesMsgs(downloadDuration, depDownloadMsgKey)
    val slowestFailedDownloads = getSlowestFilesMsgs(failedDownloadDuration, "failed")
    val slowestFetches = getSlowestFilesMsgs(fetchDuration, "fetched")
    val brokenFilesMsg =
      if (brokenFiles.nonEmpty) Seq(s"broken files: ${brokenFiles.size}", s"${brokenFiles.mkString(", ")}") else Nil
    val brokenMetadataMsg =
      if (brokenMetadata.nonEmpty) Seq(s"broken metadata: ${brokenMetadata.size}", s"${brokenMetadata.mkString(", ")}")
      else Nil
    val failedMetadataMsg =
      if (failedMetadata.nonEmpty) Seq(s"failed metadata: ${failedMetadata.size}", s"${failedMetadata.mkString(", ")}")
      else Nil

    val (libsTime, docsTime, srcsTime) =
      (
        downloadDuration.filter { case (k, time) => !k.contains(JavaDocKey) && !k.contains(SourceKey) },
        downloadDuration.filter(_._1.contains(JavaDocKey)),
        downloadDuration.filter(_._1.contains(SourceKey)))
    val (jarsTime, pomsTime) = libsTime.partition(_._1.endsWith(JarKey))

    val totalDownloadTime = downloadDuration.values.sum
    val totalFetchTime = fetchDuration.values.sum
    val totalFailedDownloadTime = failedDownloadDuration.values.sum

    val (downloadedFiles, downloadedDocs, downloadedSrcs) = (
      downloadedHttpFiles.filter(d => !d.contains(JavaDocKey) && !d.contains(SourceKey)),
      downloadedHttpFiles.filter(_.contains(JavaDocKey)),
      downloadedHttpFiles.filter(_.contains(SourceKey)))
    val (downloadedJars, downloadedPoms) = downloadedFiles.partition(_.endsWith(JarKey))

    if (downloadedFiles.nonEmpty) {
      ObtTrace.addToStat(ObtStats.TotalDepDownloadTime, totalDownloadTime)
      ObtTrace.addToStat(ObtStats.TotalDepFailedDownloadTime, totalFailedDownloadTime)
      ObtTrace.addToStat(ObtStats.TotalDepFetchTime, totalFetchTime)
    }

    val summaryLines = Seq(
      "Dependency Download Summary:",
      s"total downloaded http&https files: ${downloadedHttpFiles.size}(jars: ${downloadedJars.size}, poms: ${downloadedPoms.size}, docs: ${downloadedDocs.size}, sources: ${downloadedSrcs.size})",
      s"total download time: ${timeStr(totalDownloadTime)}(jars: ${timeStr(jarsTime.values.sum)}, poms: ${timeStr(
          pomsTime.values.sum)}, docs: ${timeStr(docsTime.values.sum)}, sources: ${timeStr(srcsTime.values.sum)})",
    ) ++ slowestDownloads ++
      (s"total non-critical failed download time: ${timeStr(failedDownloadDuration.values.sum)}" +: slowestFailedDownloads) ++
      Seq(s"total fetch time: ${timeStr(totalFetchTime)}") ++ slowestFetches ++
      brokenFilesMsg ++ brokenMetadataMsg ++ failedMetadataMsg

    clean() // be ready for next build
    summaryLines
  }
}
