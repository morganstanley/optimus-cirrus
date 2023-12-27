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
package optimus.buildtool.compilers

import java.io.StringReader
import java.nio.file.Files
import java.util.Properties
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.GenericFilesArtifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.GenericFiles
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.AsyncUtils
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

/**
 * Given a structure (where `StaticConfig.string("genericTopLevel")` == "wibble"):
 *
 * [module]/src/main/files/template/foo.txt
 * [module]/src/main/files/template/bar.txt
 *
 * [module]/src/main/files/env-shared/all.properties
 * [module]/src/main/files/env-shared/prod.properties
 * [module]/src/main/files/env-shared/ln.properties
 * [module]/src/main/files/env-shared/ln/template/baz.txt
 * [module]/src/main/files/env-shared/wibble.properties
 * [module]/src/main/files/env-shared/wibble/template/wobble.txt
 *
 * [module]/src/main/files/env/prod_ln.properties
 * [module]/src/main/files/env/prod_ln/more-tokens.properties
 * [module]/src/main/files/env/prod.properties
 *
 * It will create a jar with the contents for env 'prod_ln' and 'prod' respectively:
 * common/prod_ln/foo.txt
 * common/prod_ln/bar.txt
 * common/prod_ln/baz.txt
 * common/prod_ln/wobble.txt
 *
 * common/prod/foo.txt
 * common/prod/bar.txt
 *
 * where the files in 'prod_ln' will have tokens of the form @FOO@ replaced by the specified values from (in increasing
 * priority order):
 *   1. env-shared/all.properties
 *   2. env-shared/prod.properties
 *   3. env-shared/ln.properties
 *   4. env/prod_ln.properties
 *   5. env/prod_ln/more-tokens.properties
 *   6. env-shared/wibble.properties
 *
 * and the files in 'prod' will have tokens of the form @FOO@ replaced by the specified values from (in increasing
 * priority order):
 *   1. env-shared/all.properties
 *   2. env-shared/prod.properties
 *   3. env/prod.properties
 */
@entity class GenericFilesPackager(installVersion: String) {
  import GenericFilesPackager._

  @node def files(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[GenericFilesArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    if (content.nonEmpty) {
      ObtTrace.traceTask(scopeId, GenericFiles) {
        // Important: name of the module matters as it's used in templates
        val buildProperties = Map(
          "meta" -> scopeId.meta,
          "project" -> scopeId.bundle,
          "release" -> installVersion
        )

        val templateFiles = content.filterKeysNow(startsWith("template")).flatMap { case (id, c) =>
          id.sourceFolderToFilePath.subPath(1).map(_ -> c) // drop "template"
        }

        val environments = envParts(content, "env").map { case (name, tokens, envTemplateFiles) =>
          name -> Environment(name, tokens, envTemplateFiles)
        }.toMap

        val sharedEnvironments = envParts(content, "env-shared").map { case (name, tokens, envTemplateFiles) =>
          name -> SharedEnvironment(name, tokens, envTemplateFiles)
        }.toMap

        val nonTemplateFiles = content.filterKeysNow { id =>
          !startsWith("template")(id) && !startsWith("env")(id) && !startsWith("env-shared")(id)
        }

        val artifact = Utils.atomicallyWrite(jarPath) { tempOut =>
          // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
          val tempJar = new ConsistentlyHashedJarOutputStream(Files.newOutputStream(tempOut), None, compressed = true)
          AsyncUtils.asyncTry {
            val common = RelativePath("common")

            nonTemplateFiles.foreach { case (id, content) =>
              tempJar.copyInFile(content.contentAsInputStream, common.resolvePath(id.sourceFolderToFilePath))
            }

            val msgs =
              environments.values
                .flatMap(e =>
                  processEnvironment(tempJar, e, templateFiles, environments, sharedEnvironments, buildProperties))
                .toIndexedSeq

            val a = GenericFilesArtifact.create(scopeId, jarPath, msgs)
            import optimus.buildtool.artifacts.JsonImplicits._
            AssetUtils.withTempJson(GenericFilesArtifact.Cached(a.messages, a.hasErrors))(
              tempJar.writeFile(_, RelativePath(GenericFilesArtifact.messages))
            )
            a
          } asyncFinally tempJar.close()
        }

        Some(artifact)
      }
    } else None
  }

  private def startsWith(prefix: String)(id: SourceUnitId): Boolean =
    id.sourceFolderToFilePath.pathString.startsWith(s"$prefix/")

  private def endsWith(extension: String)(id: SourceUnitId): Boolean =
    id.sourceFolderToFilePath.pathString.endsWith(s".$extension")

  private def extractEnv(prefix: String, id: SourceUnitId): Option[RelativePath] = {
    if (endsWith("properties")(id) && id.sourceFolderToFilePath.parent.pathString.equals(prefix)) {
      // Extract env from property files, such as env/prod_ny.properties or env-shared/prod.properties
      id.sourceFolderToFilePath.name.split('.').headOption.map { envName =>
        id.sourceFolderToFilePath.parent.resolvePath(envName)
      } // eg. Some(env/prod_ny) or Some(env-shared/prod)
    } else
      /* Extract env from two cases:
       * 1. From template files, such as env-shared/prod/template/bin/foo.sh
       * 2. From specific token files, such as env/prod_ny/db.properties
       */
      id.sourceFolderToFilePath.first(2) // eg. Some(env-shared/prod) or Some(env/prod_ny)
  }

  private def envParts(
      files: Map[SourceUnitId, HashedContent],
      prefix: String
  ): Seq[(String, Map[SourceUnitId, HashedContent], Map[RelativePath, HashedContent])] =
    files
      .filterKeysNow(startsWith(prefix))
      .groupBy { case (id, _) =>
        extractEnv(prefix, id)
      }
      .collect { case (Some(envPath), contents) =>
        val envName = envPath.name

        val tokens = contents.filterKeysNow(endsWith("properties"))

        val envTemplateFileRoot = envPath.resolvePath("template").pathString
        val envTemplateFiles = contents.filterKeysNow(startsWith(envTemplateFileRoot)).flatMap { case (id, c) =>
          id.sourceFolderToFilePath
            .subPath(3)
            .map(_ -> c) // drop "env/prod_ny/template" or "env-shared/prod/template"
        }

        (envName, tokens, envTemplateFiles)
      }
      .toIndexedSeq

  private def processEnvironment(
      jar: ConsistentlyHashedJarOutputStream,
      env: Environment,
      templateFiles: Map[RelativePath, HashedContent],
      environments: Map[String, Environment],
      sharedEnvironments: Map[String, SharedEnvironment],
      buildProperties: Map[String, String]
  ): Seq[CompilationMessage] = {
    def merge[A, B](s: Seq[Map[A, B]]): Map[A, B] = s.reduceLeftOption(_ ++ _).getOrElse(Map.empty)

    def flattenEnvTemplates(envs: Seq[EnvironmentTemplate]): Map[RelativePath, HashedContent] =
      merge(envs.map(_.templateFiles))

    val envProperty = Map("env" -> env.name)
    val lowPrecedenceEnvs = env.lowPrecedence.flatMap(sharedEnvironments.get)
    val lowPrecedenceTokens = lowPrecedenceEnvs.map { e =>
      tokens(e.rawTokens)
    }

    val mediumPrecedenceEnvs = env.mediumPrecedence.flatMap(environments.get)
    val mediumPrecedenceTokens = mediumPrecedenceEnvs.map { e =>
      tokens(e.rawTokens)
    }

    val highPrecedenceEnvs = env.highPrecedence.flatMap(sharedEnvironments.get)
    val highPrecedenceTokens = highPrecedenceEnvs.map { e =>
      tokens(e.rawTokens)
    }

    val allTokens = merge(
      lowPrecedenceTokens ++ mediumPrecedenceTokens ++ highPrecedenceTokens :+ buildProperties :+ envProperty)

    resolve(allTokens) match {
      case Left(errors) =>
        errors
      case Right(tokens) =>
        // order is important here
        val envTemplateFiles =
          flattenEnvTemplates(lowPrecedenceEnvs) ++ flattenEnvTemplates(mediumPrecedenceEnvs) ++ flattenEnvTemplates(
            highPrecedenceEnvs)

        val files = templateFiles ++ envTemplateFiles
        copyFiles(jar, env.name, files, tokens)
    }
  }

  // using Properties because it's an easy way to create a Map[String, String] type structure
  // from a raw file of:
  // token1=value1
  // token2=value2
  private def tokens(rawTokens: Map[SourceUnitId, HashedContent]): Map[String, String] = {
    rawTokens.flatMap { case (_, c) =>
      val p = new Properties
      p.load(new StringReader(c.utf8ContentAsString))
      p.asScala
    }
  }

  private def resolve(tokens: Map[String, String]): Either[Seq[CompilationMessage], Map[String, String]] = {
    def partition(ts: Map[String, String]): (Map[String, String], Map[String, String]) =
      ts.partition { case (_, v) => !GenericFilesPackager.Unresolved.pattern.matcher(v).find }

    @tailrec def inner(
        resolved: Map[String, String],
        unresolved: Map[String, String]
    ): Either[Seq[CompilationMessage], Map[String, String]] = {
      val partiallyResolved = tokens.foldLeft(tokens) { case (ts, (kTok, vTok)) =>
        ts.map { case (k, v) =>
          k -> v.replace(s"@$kTok@", vTok)
        }
      }
      val (newlyResolved, stillUnresolved) = partition(partiallyResolved)
      if (stillUnresolved.isEmpty)
        // we're done
        Right(resolved ++ partiallyResolved)
      else if (stillUnresolved == unresolved)
        // we're not making progress
        Left(error(s"Unresolved tokens: ${stillUnresolved.map { case (k, v) => s"$k=$v" }.mkString("[", ", ", "]")}"))
      else inner(resolved ++ newlyResolved, stillUnresolved)
    }

    val (resolved, unresolved) = partition(tokens)
    if (unresolved.nonEmpty) inner(resolved, unresolved)
    else Right(tokens)
  }

  private def error(message: String) = Seq(CompilationMessage(None, message, CompilationMessage.Error))

  private def copyFiles(
      jar: ConsistentlyHashedJarOutputStream,
      env: String,
      files: Map[RelativePath, HashedContent],
      replacements: Map[String, String]
  ): Seq[CompilationMessage] = {
    val root = RelativePath(s"common/$env")
    files.toIndexedSeq.flatMap { case (p, c) =>
      // not sure how efficient this is - might need a better algorithm
      val content: String = replacements.foldLeft(c.utf8ContentAsString) { case (c, (k, v)) =>
        c.replace(s"@$k@", v)
      }
      jar.writeFile(content, root.resolvePath(p))
      Nil
    }
  }
}

object GenericFilesPackager {

  /**
   * An env instance from folder env could be either a single env (e.x. bas) or a compound env (e.x. bas_ln, bas_ln).
   * For a env instance, we build its related env with three types of precedence as tokens from higher precedence
   * overwrite those from lower precedence:
   *   1. Low precedence which comes from folder env-shared
   *   2. Medium precedence which comes form folder env
   *   3. High precedence which is a special one called `StaticConfig.string("genericTopLevel")` from folder env-shared
   *
   * For a single env, such as bas, its precedence will be:
   *   1. Low precedence: env-shared/all, env-shared/bas
   *   2. Medium precedence: env/bas
   *   3. High precedence: N/A
   *
   * For a compound env, such as bas_ln, its precedence will be:
   *   1. Low precedence: env-shared/all, env-shared/bas, env-shared/ln
   *   2. Medium precedence: env/bas_ln
   *   3. High precedence: env-shared/`StaticConfig.string("genericTopLevel")`
   */
  trait EnvironmentTemplate {
    def name: String
    def rawTokens: Map[SourceUnitId, HashedContent]
    def templateFiles: Map[RelativePath, HashedContent]
  }

  final case class Environment(
      name: String,
      rawTokens: Map[SourceUnitId, HashedContent],
      templateFiles: Map[RelativePath, HashedContent]
  ) extends EnvironmentTemplate {
    def isCompoundEnv: Boolean = {
      if (name.contains("_")) true
      else false
    }

    def lowPrecedence: Seq[String] = {
      if (isCompoundEnv) ("all" +: name.split("_")).toIndexedSeq
      else Seq("all", name)
    }
    def mediumPrecedence: Seq[String] = Seq(name)
    def highPrecedence: Seq[String] = {
      if (isCompoundEnv) Seq(StaticConfig.string("genericTopLevel"))
      else Nil
    }
  }

  final case class SharedEnvironment(
      name: String,
      rawTokens: Map[SourceUnitId, HashedContent],
      templateFiles: Map[RelativePath, HashedContent]
  ) extends EnvironmentTemplate

  final case class Inputs(jarPath: JarAsset, content: Map[SourceUnitId, HashedContent])

  private val Unresolved = "@\\w+@".r
}
