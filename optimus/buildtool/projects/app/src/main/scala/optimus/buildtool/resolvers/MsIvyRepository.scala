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

import coursier.core._
import coursier.ivy._
import coursier.util.Artifact
import coursier.util.EitherT
import coursier.util.Monad
import optimus.buildtool.config.DependencyCoursierKey
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.Exclude
import optimus.buildtool.utils.PathUtils
import optimus.platform.util.xml.UnsafeXML
import optimus.platform._

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.language.higherKinds
import scala.util.Try

/**
 * Copy of coursier.ivy.IvyRepository (which sadly isn't extensible) with some minor changes to support AFS Ivy.
 *
 * TODO (OPTIMUS-26823): Remove the code duplication
 */
private[resolvers] final case class MsIvyRepository(
    ivyPattern: Pattern,
    artifactPatterns: Seq[Pattern],
    changing: Option[Boolean],
    withChecksums: Boolean,
    withSignatures: Boolean,
    withArtifacts: Boolean,
    // hack for SBT putting infos in properties
    dropInfoAttributes: Boolean,
    authentication: Option[Authentication],
    hasMeta: Boolean,
    fileSystem: FileSystem,
    excludes: Map[DependencyCoursierKey, Seq[Exclude]],
    afsGroupNameToMavenMap: Map[(String, String), Seq[DependencyDefinition]]
) extends Repository {

  import MsIvyRepository._

  lazy val revisionListingPatternOpt: Option[Pattern] = {
    val idx = ivyPattern.chunks.indexWhere { chunk =>
      chunk == Pattern.Chunk.Var("revision")
    }

    if (idx < 0)
      None
    else
      Some(Pattern(ivyPattern.chunks.take(idx)))
  }

  import coursier.core.Repository._

  private def variables(
      module: Module,
      versionOpt: Option[String],
      `type`: String,
      artifact: String,
      ext: String,
      classifierOpt: Option[String]
  ) = {
    val standardVars = Map[String, String](
      "organization" -> module.organization.value,
      "organisation" -> module.organization.value,
      "orgPath" -> module.organization.value.replace('.', '/'),
      "module" -> module.name.value,
      "type" -> `type`,
      "artifact" -> artifact,
      "ext" -> ext
    ) ++
      module.attributes ++
      classifierOpt.map("classifier" -> _).toSeq ++
      versionOpt.map("revision" -> _).toSeq

    if (hasMeta) {
      val dotIdx = module.organization.value.indexOf('.')
      val msExtraVars = if (dotIdx > 0) {
        val (meta, project) = module.organization.value.splitAt(dotIdx)
        Map("meta" -> meta, "project" -> project.tail)
      } else Map("meta" -> "NO_META", "project" -> "NO_PROJECT")

      standardVars ++ msExtraVars
    } else standardVars
  }

  private val EmptyArtifactSource = new ArtifactSource {
    override def artifacts(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[scala.Seq[Classifier]]): scala.Seq[(Publication, Artifact)] = Nil
  }
  val source: ArtifactSource =
    if (withArtifacts) MsIvyArtifactSource else EmptyArtifactSource

  @entity object MsIvyArtifactSource extends ArtifactSource with AsyncArtifactSource with AfsArtifactSource {

    private val finder = FileFinder(fileSystem)
    override def artifacts(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[scala.Seq[Classifier]]
    ): scala.Seq[(Publication, Artifact)] =
      throw new IllegalStateException("I'm expecting to be called by CoursierArtifactResolver on artifactsNode")

    @node def artifactsNode(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[Seq[String]]
    ): Seq[Either[String, CoursierArtifact]] = artifactsImpl(dependency, project, overrideClassifiers)

    // split the implementation out into a non-node method since we're only using @node for caching here;
    // there are no async transforms needed below
    private def artifactsImpl(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[Seq[String]]
    ): Seq[Either[String, CoursierArtifact]] = {
      val publications = matchingPublications(dependency, project, overrideClassifiers)
      val namedArtifacts: Seq[Either[String, CoursierArtifact]] = toArtifacts(dependency, project, publications)

      val nonClassArtifacts = this.nonClassArtifacts(dependency, project, publications, namedArtifacts)

      val artifacts: Seq[CoursierArtifact] = namedArtifacts
        .flatMap(_.toOption)
        .map { case artifact =>
          if (isClassJar(artifact)) {
            // attach extra information to the class jar if available
            val extraArtifacts = nonClassArtifacts.getOrElse(artifact.publication.name, Nil)
            if (extraArtifacts.nonEmpty)
              artifact.copy(
                value = artifact.value.withExtra(
                  artifact.value.extra ++ extraArtifacts.map(a => a.`type`.value -> a.value).toSingleMap
                ))
            else artifact
          } else artifact
        }

      val errors = namedArtifacts.collect { case e @ Left(_) => e }

      (artifacts.map(Right(_)) ++ errors).sortBy {
        // sort so that we get consistent results on every run (important for stable fingerprints and compilations)
        case Left(err)       => err
        case Right(artifact) => artifact.url
      }
    }

    private def matchingPublications(
        dependency: Dependency,
        project: Project,
        overrideClassifiers: Option[Seq[String]]
    ) = overrideClassifiers match {
      case None =>
        if (dependency.attributes.classifier.nonEmpty) {
          project.publications.collect {
            case (_, p) if p.classifier == dependency.attributes.classifier =>
              p
          }
        } else if (dependency.attributes.`type`.nonEmpty) {
          project.publications.collect {
            case (_, p)
                if p.classifier.isEmpty && (
                  p.`type` == dependency.attributes.`type` ||
                    (p.ext.value == dependency.attributes.`type`.value && project.packagingOpt.toSeq.contains(
                      p.`type`
                    )) // wow
                ) =>
              p

          }
        } else {
          project.publications.collect {
            case (conf, p)
                if conf.value == "*" ||
                  conf == dependency.configuration ||
                  project.allConfigurations.getOrElse(dependency.configuration, Set.empty).contains(conf) =>
              p
          }
        }
      case Some(classifiers) =>
        val classifiersSet = classifiers.toSet
        project.publications.collect {
          case (_, p) if classifiersSet(p.classifier.value) =>
            p
        }
    }

    private def nonClassArtifacts(
        dependency: Dependency,
        project: Project,
        retainedPublications: Seq[Publication],
        retainedArtifacts: Seq[Either[String, CoursierArtifact]]
    ): Map[String, Seq[CoursierArtifact]] = {
      val otherNonClassPublications = project.publications.collect {
        case (_, p) if !isClassJar(p) && !retainedPublications.contains(p) =>
          p
      }
      val otherNonClassArtifacts = toArtifacts(dependency, project, otherNonClassPublications)

      (retainedArtifacts ++ otherNonClassArtifacts).collect {
        case Right(artifact) if !isClassJar(artifact) => (artifact.publication.name, artifact)
      }.toGroupedMap
    }

    private def toArtifacts(
        dependency: Dependency,
        project: Project,
        publications: Seq[Publication]
    ): Seq[Either[String, CoursierArtifact]] =
      publications.distinct.map { publication =>
        val uris =
          artifactPatterns
            .flatMap(
              _.substituteVariables(variables(
                dependency.module,
                Some(project.actualVersion),
                publication.`type`.value,
                publication.name,
                publication.ext.value,
                Some(publication.classifier.value).filter(_.nonEmpty)
              )).toOption)

        finder
          .findFirstExisting(uris)
          .map { uri =>
            var artifact = Artifact(
              uri,
              Map.empty,
              Map.empty,
              changing = changing.getOrElse(project.version.contains("-SNAPSHOT")), // could be more reliable
              authentication = authentication,
              optional = false
            )

            if (withChecksums)
              artifact = artifact.withDefaultChecksums
            if (withSignatures)
              artifact = artifact.withDefaultSignature

            Right(CoursierArtifact(artifact, publication.withType(correctedType(publication))))
          }
          .getOrElse(
            Left(s"No artifact found for ${publication.name}:${publication.`type`.value}; tried ${uris.mkString(", ")}")
          )
      }
  }

  def artifacts(
      dependency: Dependency,
      project: Project,
      overrideClassifiers: Option[scala.Seq[Classifier]]
  ): scala.Seq[(Publication, Artifact)] =
    throw new IllegalStateException("I'm expecting not to be called")

  def find[F[_]](
      module: Module,
      version: String,
      fetch: Repository.Fetch[F]
  )(implicit F: Monad[F]): EitherT[F, String, (ArtifactSource, Project)] = {

    revisionListingPatternOpt match {
      case None =>
        findNoInverval(module, version, fetch)
      case Some(revisionListingPattern) =>
        Parse
          .versionInterval(version)
          .orElse(Parse.multiVersionInterval(version))
          .orElse(Parse.ivyLatestSubRevisionInterval(version))
          .filter(_.isValid) match {
          case None =>
            findNoInverval(module, version, fetch)
          case Some(itv) =>
            EitherT(
              F.point(
                Left(s"Version '$version' is not valid. (Note: Interval version lookup not supported with AFS Ivy)")))
        }
    }
  }

  def findNoInverval[F[_]](
      module: Module,
      version: String,
      fetch: Repository.Fetch[F]
  )(implicit F: Monad[F]): EitherT[F, String, (ArtifactSource, Project)] = {

    val eitherArtifact: Either[String, Artifact] =
      for {
        url <- ivyPattern.substituteVariables(
          variables(module, Some(version), "ivy", "ivy", "xml", None)
        )
      } yield {
        var artifact = Artifact(
          url,
          Map.empty,
          Map.empty,
          changing = changing.getOrElse(version.contains("-SNAPSHOT")),
          authentication = authentication,
          optional = false
        )

        if (withChecksums)
          artifact = artifact.withDefaultChecksums
        if (withSignatures)
          artifact = artifact.withDefaultSignature

        artifact
      }

    for {
      artifact <- EitherT(F.point(eitherArtifact))
      ivy <- fetch(artifact)
      proj0 <- EitherT(F.point {
        for {
          xml <- Try(UnsafeXML(disallowDoctypeDecl = false).loadString(compatibility.xmlPreprocess(ivy)))
            .fold(t => Left(t.getMessage), e => Right(compatibility.xmlFromElem(e)))
          _ <- if (xml.label == "ivy-module") Right(()) else Left("Module definition not found")
          proj <- MsIvyXml.project(xml, version, excludes, afsGroupNameToMavenMap)
        } yield proj
      })
    } yield {
      val proj =
        if (dropInfoAttributes)
          proj0
            .withModule(
              proj0.module.withAttributes(
                proj0.module.attributes.filter { case (k, _) =>
                  !k.startsWith("info.")
                }
              ))
            .withDependencies(proj0.dependencies.map { case (config, dep0) =>
              val dep = dep0.withModule(
                dep0.module.withAttributes(
                  dep0.module.attributes.filter { case (k, _) =>
                    !k.startsWith("info.")
                  }
                )
              )

              config -> dep
            })
        else
          proj0

      source -> proj.withActualVersionOpt(Some(version))
    }
  }

}

private[resolvers] object MsIvyRepository {
  def parse(
      ivyPatternStr: String,
      artifactPatternStrs: Seq[String],
      changing: Option[Boolean] = None,
      properties: Map[String, String] = Map.empty,
      withChecksums: Boolean = true,
      withSignatures: Boolean = true,
      withArtifacts: Boolean = true,
      // hack for SBT putting infos in properties
      dropInfoAttributes: Boolean = false,
      authentication: Option[Authentication] = None,
      fileSystem: FileSystem = FileSystems.getDefault,
      excludes: Map[DependencyCoursierKey, Seq[Exclude]] = Map.empty,
      afsGroupNameToMavenMap: Map[(String, String), Seq[DependencyDefinition]] = Map.empty
  ): Either[String, MsIvyRepository] =
    for {
      ivyPropertiesPattern <- PropertiesPattern.parse(ivyPatternStr)
      artifactPropertiesPattern <- artifactPatternStrs.foldLeft[Either[String, ListBuffer[PropertiesPattern]]](
        Right(ListBuffer.empty)) { (acc, pattern) =>
        acc.flatMap(accr => PropertiesPattern.parse(pattern).map(ppr => accr += ppr))
      }

      ivyPattern <- ivyPropertiesPattern.substituteProperties(properties)
      artifactPatterns <- artifactPropertiesPattern.foldLeft[Either[String, ListBuffer[Pattern]]](
        Right(ListBuffer.empty)) { (a, b) =>
        a.flatMap(ar => b.substituteProperties(properties).map(ppr => ar += ppr))
      }
    } yield MsIvyRepository(
      ivyPattern,
      artifactPatterns,
      changing,
      withChecksums,
      withSignatures,
      withArtifacts,
      dropInfoAttributes,
      authentication,
      hasMeta = containsMetaProj(ivyPatternStr) || artifactPatternStrs.exists(containsMetaProj),
      fileSystem,
      excludes,
      afsGroupNameToMavenMap
    )

  private def containsMetaProj(s: String) = s.contains("[meta]") || s.contains("[project]")

  def isClassJar(pub: Publication): Boolean = isClassJar(correctedType(pub))

  def correctedType(pub: Publication): Type = {
    val tpe = pub.`type`
    if (isClassJar(tpe) && pub.ext.value == "src.jar") Type.source // some src jars are marked as jars by mistake
    else if (tpe.value == "src.jar") Type.source // eg. kdbsf_api
    else tpe
  }

  def isClassJar(artifact: CoursierArtifact): Boolean = isClassJar(artifact.attributes.`type`)

  def isClassJar(tpe: Type): Boolean =
    tpe.value == "jar" || tpe.value == "bundle" // (see https://stackoverflow.com/questions/5389691/what-is-the-meaning-of-type-bundle-in-a-maven-dependency)
}

@entity private class FileFinder(fileSystem: FileSystem) {
  @node def uriExists(uri: String): Boolean =
    Files.exists(PathUtils.uriToPath(uri, fileSystem))

  // (it's not a real sync stack because uriExists isn't really async)
  @entersGraph def findFirstExisting(uris: Seq[String]): Option[String] =
    uris.aseq.find(uriExists)
}

@entity trait AsyncArtifactSource {
  @node def artifactsNode(
      dependency: Dependency,
      project: Project,
      overrideClassifiers: Option[Seq[String]]
  ): Seq[Either[String, CoursierArtifact]]
}

// marker trait
trait AfsArtifactSource