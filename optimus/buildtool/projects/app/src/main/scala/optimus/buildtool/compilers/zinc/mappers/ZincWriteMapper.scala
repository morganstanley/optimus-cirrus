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
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.zinc.ZincUtils._
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.MessageTrace
import optimus.platform.util.Log
import org.apache.commons.lang3.StringUtils
import xsbti.VirtualFile
import xsbti.compile.analysis.WriteMapper

/** Mapper for the current scope's signature analysis file. */
private[zinc] object ZincSignatureWriteMapper extends PathTranslatingMapper with WriteMapper with Log {
  // no files should be present in signature analysis
  override protected def translateFile(file: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincSignatureWriteMapper for file: $file")
  // no options should be present in signature analysis
  override protected def translateOptions(text: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincSignatureWriteMapper for option: $text")
}

/** Mapper for the current scope's java/scala analysis file. */
class ZincWriteMapper(
    val scopeId: ScopeId,
    fingerprintHash: String,
    val traceType: MessageTrace,
    val outputJar: PathPair,
    val workspaceRoot: Directory,
    val buildDir: Directory,
    val depCopyRoot: Directory,
    val strictErrorTolerance: Boolean,
    coreClasspath: Seq[VirtualFile],
    val depCopyFileSystemAsset: Boolean
) extends ZincPathTranslatingMapper
    with WriteMapper {

  // On output, all we need to do is strip out the UUID-hyphen.
  override protected final def translateFile(path: String): String =
    dissectFile(buildDir, Asset.parse(buildDir.fileSystem, path)) match {
      // Strip scala and jvm jars out of the analysis (to prevent invalidations due to switching
      // between graal and openjdk for example)
      case Left(file) if coreClasspath.exists(_.id == file.pathString) =>
        DUMMY
      case Left(file) =>
        val sanitizedPath = externalDepsSubstitutions.foldLeft(file.pathString) { case (result, s) =>
          result.replace(s.realDirectory, s.key) // convert dir path to KEY
        }

        validatePath(sanitizedPath)
        sanitizedPath
      // For current scope, strip out the uuid and specific working directory.
      case Right(Dissection(_, `classType`, Some(uuid), `scopeName`, hash, "jar", fileInJar)) =>
        assert(uuid == this.uuid)
        assert(hash == fingerprintHash)
        reconstructFile(BUILD_DIR, classType, None, scopeName, hash, fileInJar).pathString
      // For inputs used for this build, just substitute the stand-in BUILD directory.
      case Right(Dissection(_, tpe, None, name, hash, "jar", fileInJar)) =>
        reconstructFile(BUILD_DIR, tpe, None, name, hash, fileInJar).pathString
      case x =>
        throw new RuntimeException(s"Unexpected file $x")
    }

  var previousArg: Option[String] = None
  override protected final def translateOptions(text: String): String = {
    // Restore the buildDir and depcopyRoot for obt files
    assert(!text.contains(BUILD_DIR.pathString), "Input analyses should not contain stand-in BUILD")
    assert(!text.contains(DEPCOPY), "Input analyses should not contain stand-in DEPCOPY")

    val isXPlugin = text.startsWith(pluginFlag)
    val isYMacro = previousArg.contains(macroFlag)
    val isPickleWrite = previousArg.contains(pickleWriteFlag)

    val ret =
      if (isPickleWrite) {
        // -Ypickle-write option is ignored when checking equivalence (since we add it
        // to IncToolOptions.ignoredScalacOptions), but to keep the analysis files
        // truly independent we replace it with a fixed string anyway
        DUMMY
      } else {
        def separatorSubstitutions(s: String): String =
          if ((isXPlugin || isYMacro) && File.pathSeparator != ":")
            s.replace(File.pathSeparator, ":")
          else s

        def buildSubstitutions(s: String): String = {
          replaceBuildJarsInText(buildDir, s) { jarAsset =>
            translateOptionPath(jarAsset).pathString
          }
        }

        val pathSubstitutions: Seq[Substitution] =
          if (isXPlugin || isYMacro) externalDepsSubstitutions
          else Seq(workspaceSubstitution)

        val substitutions = pathSubstitutions.filterNot(_.isSame)
        StringUtils.replaceEach(
          buildSubstitutions(separatorSubstitutions(text)),
          substitutions.map(_.realDirectory).toArray,
          substitutions.map(_.key).toArray
        )
      }
    previousArg = Some(text)
    ret
  }

  private def translateOptionPath(jar: JarAsset): FileAsset = {
    dissectFile(buildDir, jar) match {
      case Right(Dissection(_, tpe, Some(uuid), `scopeName`, hash, "jar", None)) =>
        assert(uuid == this.uuid)
        assert(hash == fingerprintHash)
        reconstructFile(BUILD_DIR, tpe, None, scopeName, hash, None)
      case Right(Dissection(_, tpe, _, name, hash, "jar", None)) =>
        reconstructFile(BUILD_DIR, tpe, None, name, hash, None)
      case _ =>
        validatePath(jar.pathString)
        jar
    }
  }
}

/**
 * Mapper for other scopes' analysis files (signature, scala and java). This should never be called, since we only read
 * analysis for other scopes, not write.
 */
object ZincExternalWriteMapper extends PathTranslatingMapper with WriteMapper {
  override protected def translateFile(file: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincExternalWriteMapper for file: $file")
  override protected def translateOptions(text: String): String =
    throw new UnsupportedOperationException(s"Unexpected call to ZincExternalWriteMapper for option: $text")
}
