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

import coursier.core
import coursier.core.ArtifactSource
import coursier.core.Authentication
import coursier.core.Classifier
import coursier.core.Dependency
import coursier.core.Extension
import coursier.core.Info
import coursier.core.Module
import coursier.core.Project
import coursier.core.Publication
import coursier.core.Repository
import coursier.core.Repository.Fetch
import coursier.core.Type
import coursier.ivy.Pattern
import coursier.ivy.PropertiesPattern
import coursier.util.Artifact
import coursier.util.EitherT
import coursier.util.Monad
import coursier.util.Sync

import java.net.URL
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.NamingConventions.MavenUnzipFileKey
import optimus.buildtool.resolvers.MavenUtils._
import optimus.buildtool.trace.ObtTrace
import optimus.platform._
import optimus.stratosphere.artifactory.Credential

/**
 * This is a customized ivyRepo to make obt support download maven file in compressed archives(.tar), it will take
 * advantage from directly access files inside of compressed archive without full download.
 *
 * To use this, need to add file information in 'resolvers.obt' & 'dependencies.obt', for example: for
 * 'https://my-host/maven/generic-3rdparty-local/intellij/ideaIC-2021.3.1.tar.gz!/idea-IC-213.6461.79/lib/idea.jar'
 *
 * # get intellij .jar file in compressed achieve # urls = root + ivys + aftertar + artifacts
 *   1. In 'resolvers.obt': At line 1 add 'resolvers = .... , ${ideaRepo}',
 *
 * and add: ideaRepo = { name = "intellij-maven" root = "http://my-host/maven/generic-3rdparty-local" tar \=
 * "[organisation]/[module]-[version].tar.gz" ivys = [${ideaRepo.tar}] artifacts = [
 * ${ideaRepo.tar}"!/"${versions.intellij.detail}"/lib/[configuration].jar"
 * ${ideaRepo.tar}"!/"${versions.intellij.detail}"/lib/ant/lib/[configuration].jar"
 * ${ideaRepo.tar}"!/"${versions.intellij.detail}"/plugins/java/lib/[configuration].jar"
 * ${ideaRepo.tar}"!/"${versions.intellij.detail}"/plugins/junit/lib/[configuration].jar"
 * ${ideaRepo.tar}"!/"${versions.intellij.detail}"/plugins/git4idea/lib/[configuration].jar" ] }
 *
 * 2. In 'dependencies.obt', add new file dependency: intellij.ideaIC { version = "2021.3.1" configurations =[ idea,
 * annotations, kotlin-stdlib-jdk8, platform-impl, idea-junit] }
 */
private[resolvers] final case class UnzipFileRepository(
    urlPattern: Pattern,
    artifactPatterns: Seq[Pattern],
    credentialOption: Option[Credential],
    remoteAssetStore: RemoteAssetStore,
    dependencyCopier: DependencyCopier
) extends Repository {
  val authentication: Option[Authentication] = credentialOption.map(x => Authentication(x.user, x.password))
  val credential: Credential = credentialOption.getOrElse(Credential("", "", ""))

  private def variables(
      module: Module,
      version: String,
      config: String = "",
      `type`: String = "",
      artifact: String = "",
      ext: String = ""
  ): Map[String, String] = Map[String, String](
    "organization" -> module.organization.value,
    "organisation" -> module.organization.value,
    "module" -> module.name.value,
    "version" -> version,
    "configuration" -> config,
    "type" -> `type`,
    "artifact" -> artifact,
    "ext" -> ext
  ) ++ module.attributes

  override def artifacts(
      dependency: Dependency,
      project: Project,
      overrideClassifiers: Option[scala.Seq[Classifier]]): scala.Seq[(Publication, Artifact)] = {
    throw new IllegalStateException("Unzip Repository 'artifacts' is invalid, should not be called")
  }

  def makeUrl(module: Module, version: String, config: String = "", pattern: Pattern = urlPattern): URL = {
    val eitherUrl = for {
      url <- pattern.substituteVariables(
        variables(module, version, config)
      )
    } yield url

    eitherUrl match {
      case Right(url) => new URL(url)
      case Left(msg)  => throw new IllegalArgumentException(msg)
    }
  }

  @node def downloadUnzip(url: URL, isMarker: Boolean, artifact: Artifact): Either[String, Artifact] =
    downloadUrl[Either[String, Artifact]](url, dependencyCopier, isMarker)(asNode(d => Right(artifact)))(Left(_))

  @entersGraph def downloadUnzipUrl(rawUrl: URL, isZip: Boolean = false): Either[String, Artifact] = {
    val isMarker = isZip || rawUrl.toString.endsWith(MavenUnzipFileKey)
    val url = if (isMarker) new URL(rawUrl.toString + ".marker") else rawUrl
    val foundArtifact = Artifact(url.toString).withAuthentication(authentication)
    // The order of checking is 1. local disk, 2. SK, 3. maven
    if (checkLocalDisk(url, dependencyCopier)) Right(foundArtifact)
    else {
      val result = downloadUnzip(url, isMarker, foundArtifact)
      if (result.isRight) ObtTrace.info(s"Downloaded remote intellij artifact to local disk: $rawUrl")
      result
    }
  }

  override def find[F[_]](module: core.Module, version: String, fetch: Fetch[F])(implicit
      F: Monad[F]): EitherT[F, String, (ArtifactSource, Project)] = {
    // we delay the checking of the URL so that coursier only checks it if the artifact failed to resolve from previous
    // repositories (otherwise we'd look for every single filesystem based dependency here too)
    val S = F.asInstanceOf[Sync[F]]
    EitherT(F.map(S.delay(downloadUnzipUrl(makeUrl(module, version), isZip = true))) {
      _.map(_ => UnzipArtifactSource -> makeProject(module, version))
    })
  }

  private def makeProject[F[_]](module: Module, version: String) = {
    Project(
      module,
      version,
      Nil,
      Map.empty,
      None,
      Nil,
      Nil,
      Nil,
      None,
      None,
      None,
      false,
      None,
      Nil,
      Info(
        s"jar file inside $MavenUnzipFileKey file",
        "",
        Nil,
        Nil,
        None,
        None
      )
    )
  }
  @entity object UnzipArtifactSource extends ArtifactSource with AsyncArtifactSource {

    override def artifacts(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[scala.Seq[Classifier]]): scala.Seq[(Publication, Artifact)] =
      if (overrideClassifiers.nonEmpty)
        Nil // should only be called to resolve maven classifier file and always return empty
      else throw new UnsupportedOperationException("Unexpected: Should not be called 'artifacts' for UnzipRepository")

    @node override def artifactsNode(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[Seq[String]]): Seq[Either[String, CoursierArtifact]] = {
      val insideFileUrls: Seq[URL] = artifactPatterns.map { artifactPattern =>
        makeUrl(dependency.module, dependency.version, dependency.configuration.value, artifactPattern)
      }

      val urlArtifacts: Seq[Either[String, CoursierArtifact]] = insideFileUrls.apar.map { url =>
        downloadUnzipUrl(url)
          .map { art =>
            val unzipUrlPublication: Publication = Publication(
              s"${dependency.module.nameWithAttributes} Maven unzip file",
              Type("jar"),
              Extension("jar"),
              Classifier(""))
            CoursierArtifact(art, unzipUrlPublication)
          }
      }

      val artifactsResult = urlArtifacts.collect { case r @ Right(_) => r }
      if (artifactsResult.nonEmpty) artifactsResult.sortBy(_.value.url)
      else urlArtifacts
    }
  }

}

private[resolvers] object UnzipFileRepository {
  def parse(
      url: String,
      artifactPatterns: Seq[String],
      credentialOption: Option[Credential],
      remoteAssetStore: RemoteAssetStore,
      dependencyCopier: DependencyCopier
  ): Either[String, UnzipFileRepository] = {
    for {
      artifactPropertiesPattern <- artifactPatterns.foldLeft[Either[String, Seq[PropertiesPattern]]](Right(Seq.empty)) {
        (acc, pattern) =>
          acc.flatMap(accr => PropertiesPattern.parse(pattern).map(ppr => accr :+ ppr))
      }
      artifactPatterns <- artifactPropertiesPattern.foldLeft[Either[String, Seq[Pattern]]](Right(Seq.empty)) { (a, b) =>
        a.flatMap(ar => b.substituteProperties(Map.empty).map(ppr => ar :+ ppr))
      }
      urlPropertiesPattern <- PropertiesPattern.parse(url)
      urlPattern <- urlPropertiesPattern.substituteProperties(Map.empty)
    } yield {
      UnzipFileRepository(urlPattern, artifactPatterns, credentialOption, remoteAssetStore, dependencyCopier)
    }
  }

}
