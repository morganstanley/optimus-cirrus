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
package optimus.buildtool.compilers.zinc.mappers

import java.io.File

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.zinc.ZincUtils._
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.Utils
import xsbti.compile.analysis.ReadMapper

import scala.collection.mutable

/**
 * Mapper for the current scope's signature analysis file. This should never be called, since we only write signature
 * analysis for our scope, not read.
 */
object ZincSignatureReadMapper extends PathTranslatingMapper with ReadMapper {
  override protected def translateFile(file: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincSignatureReadMapper for file: $file")
  override protected def translateOptions(text: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincSignatureReadMapper for option: $text")
}

object ZincReadMapper {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

/** Mapper for the current scope's java/scala analysis file. */
private[zinc] class ZincReadMapper(
    val scopeId: ScopeId,
    fingerprintHash: String,
    incremental: Boolean,
    val traceType: MessageTrace,
    val outputJar: PathPair,
    mappingTrace: MappingTrace,
    tpeAndNameToLatestIncrAndHash: Map[(ArtifactType, String), (Boolean, String)],
    val workspaceRoot: Directory,
    val buildDir: Directory,
    val depCopyRoot: Directory,
    updatePluginHash: Boolean,
    val strictErrorTolerance: Boolean,
    val depCopyFileSystemAsset: Boolean
) extends ZincPathTranslatingMapper
    with ReadMapper {
  import ZincReadMapper._

  private val prefix = Utils.logPrefix(scopeId, traceType)

  /*
      On recompile, we will potentially have different HASHes for all inputs, and our outputs (which is one of the inputs)
      will have been copied to a new TEMP prefix.  So on READ we we should
        - Change BUILD to buildDir
        - Update all HASHes to match those in the classpath.
        - Add TEMP prefix to current scope jar names.
   */

  override protected final def translateFile(path: String): String = {
    dissectFile(BUILD_DIR, Asset.parse(BUILD_DIR.fileSystem, path)) match {
      case Left(file) =>
        val path = file.pathString
        validatePath(path)
        path
          .replace(WORKSPACE, workspaceRootStr)
          .replace(HTTPSCOPY, depCopyHttpsStr) // need check https first
          .replace(HTTPCOPY, depCopyHttpStr)
          .replace(DEPCOPY, depCopyDistStr) // then go for AFS check

      // For current output, substitute in current uuid and hash, and prepend current working directory.
      case Right(Dissection(_, `classType`, _, `scopeName`, _, _, "jar", fileInJar)) =>
        reconstructFile(buildDir, classType, Some(uuid), scopeName, incremental, fingerprintHash, fileInJar).pathString
      // For other inputs, we don't expect any uuid, but we need to map to new hashes, potentially.
      case Right(Dissection(_, tpe, None, name, origIncr, origHash, "jar", fileInJar)) =>
        val (incr, hash) = updatedIncrAndHash(tpe, name, origIncr, origHash)
        reconstructFile(buildDir, tpe, None, name, incr, hash, fileInJar).pathString
      case x =>
        throw new RuntimeException(s"Unexpected file $x")
    }
  }

  var previousArg: Option[String] = None // so far, used only for debugging
  override protected final def translateOptions(text: String): String = {
    // Restore the buildDir for obt files
    val isXPlugin = text.startsWith("-Xplugin:")
    val isYMacro = previousArg.contains("-Ymacro-classpath")

    // Note this has to happen before buildSubstitutions, since buildDir may contain ":"
    // Slightly hacky here - we undo the conversion of the colon in "-Xplugin" after doing the separator replacement
    val separatorSubstitutions: Seq[(String, String)] =
      if ((isXPlugin || isYMacro) && File.pathSeparator != ":")
        Seq((":", File.pathSeparator), ("-Xplugin" + File.pathSeparator, "-Xplugin:"))
      else Seq.empty

    val buildSubstitutions: Seq[(String, String)] = {
      val jars = findBuildJarsInText(BUILD_DIR, text)
      val updateHash = updatePluginHash || !isXPlugin

      jars.map(j => (j.pathString, translateOptionPath(j, updateHash).pathString))
    }

    val pathSubstitutions: Seq[(String, String)] = Seq((DEPCOPY, depCopyDistStr), (WORKSPACE, workspaceRootStr))

    val substitutions =
      (separatorSubstitutions ++ buildSubstitutions ++ pathSubstitutions).filterNot(p => p._1 == p._2)

    val ret = substitutions.foldLeft(text) { case (acc, (from, to)) =>
      acc.replace(from, to)
    }
    previousArg = Some(text)
    ret
  }

  private def translateOptionPath(jar: JarAsset, updateHash: Boolean): FileAsset = {
    dissectFile(BUILD_DIR, jar) match {
      case Right(Dissection(_, tpe, _, `scopeName`, _, _, "jar", None)) =>
        val path = reconstructFile(buildDir, tpe, Some(uuid), scopeName, incremental, fingerprintHash, None)
        assert(tpe == ArtifactType.Scala || tpe == ArtifactType.JavaAndScalaSignatures || tpe == ArtifactType.Java)
        path
      case Right(Dissection(_, tpe, _, name, origIncr, origHash, "jar", None)) =>
        val (incr, hash) =
          if (updateHash) updatedIncrAndHash(tpe, name, origIncr, origHash) else (origIncr, origHash)
        reconstructFile(buildDir, tpe, None, name, incr, hash, None)
      case _ =>
        validatePath(jar.pathString)
        jar
    }
  }

  private val namesWithMissingHash = new mutable.HashSet[String]()
  private def updatedIncrAndHash(tpe: ArtifactType, name: String, origIncr: Boolean, origHash: String) =
    tpeAndNameToLatestIncrAndHash.get((tpe, name)) match {
      case Some((newIncr, newHash)) =>
        if (newHash == origHash) {
          // if the hash hasn't changed, then we can save time later by reusing the upstream API analysis within this
          // scope's full analysis, rather than having to read the new one from disk to see what's changed
          if (tpe == ArtifactType.JavaAndScalaSignatures) {
            // unchangedProvenances is used to short-circuit upstream API analysis lookup when:
            // - detecting initial changes to upstream classes we depend on (at which point the provenance's type
            //   always reflects the intended final jar location for the class, ie. scala or java)
            // - determining the upstream analysis to include in this scope's final analysis (at which point
            //   the provenance's type reflects the real source of the upstream class in this compilation's classpath,
            //   ie. signatures)
            // if we know the signature hash hasn't changed, then the hash for the scala and java jars will
            // be the same too (and we need to store all three in unchangedProvenances as described directly above)
            mappingTrace.unchangedProvenances +=
              MappingTrace.provenance(ArtifactType.JavaAndScalaSignatures, name, origHash)
            mappingTrace.unchangedProvenances += MappingTrace.provenance(ArtifactType.Scala, name, origHash)
            mappingTrace.unchangedProvenances += MappingTrace.provenance(ArtifactType.Java, name, origHash)
          } else mappingTrace.unchangedProvenances += MappingTrace.provenance(tpe, name, origHash)
        } else {
          mappingTrace.hashUpdatesOnRead += 1
        }
        (newIncr, newHash)
      case None if namesWithMissingHash.contains(name) =>
        (origIncr, origHash)
      case _ =>
        log.debug(
          s"${prefix}No new hash found for $name:$tpe (maybe a removed dependency); using $origHash (incremental: $origIncr)")
        namesWithMissingHash += name
        (origIncr, origHash)
    }
}

/** Mapper for other scopes' analysis files (signature, scala and java). */
object ZincExternalReadMapper extends PathTranslatingMapper with ReadMapper {
  // files and options are unused in analysis from other scopes, so just replace them with DUMMY entries
  override protected final def translateFile(file: String): String = DUMMY
  override protected def translateOptions(text: String): String = DUMMY
}
