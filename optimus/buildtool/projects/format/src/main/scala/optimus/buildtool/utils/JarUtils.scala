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
package optimus.buildtool.utils

import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import optimus.buildtool.files.JarAsset

import java.util.jar
import java.util.jar.Attributes.Name
import scala.collection.compat._
import scala.collection.immutable.Seq

object JarUtils {
  object nme {
    val ClassJar = new Name("MS-class-jar")

    val CanRetransformClasses = new Name("Can-Retransform-Classes")
    val CanSetNativeMethodPrefix = new Name("Can-Set-Native-Method-Prefix")
    val ExtraFiles: Name = new Name("MS-extra-files")
    val ExternalJniPath: Name = new Name("MS-external-jni-path")

    val JniScopes: Name = new Name("MS-jni-scopes") // used by OAR/OTR for installed libs
    val PackagedJniLibs: Name = new Name("MS-packaged-jni-libraries") // used by ObtRuntimeSupport for intellij
    val JniFallbackPath: Name =
      new Name("MS-jni-fallback-path") // used by OAR/OTR/intellij when falling back to disted libs

    val PreloadReleaseScopes: Name =
      new Name("MS-preload-release-scopes") // used by OAR/OTR for installed libs
    val PreloadDebugScopes: Name =
      new Name("MS-preload-debug-scopes") // used by OAR/OTR for installed libs
    val PackagedPreloadReleaseLibs: Name =
      new Name("MS-packaged-preload-release-libraries") // used by ObtRuntimeSupport for intellij
    val PackagedPreloadDebugLibs: Name =
      new Name("MS-packaged-preload-debug-libraries") // used by ObtRuntimeSupport for intellij
    val PreloadReleaseFallbackPath: Name =
      new Name("MS-preload-release-fallback-path") // used by OAR/OTR/intellij when falling back to disted libs
    val PreloadDebugFallbackPath: Name =
      new Name("MS-preload-debug-fallback-path") // used by OAR/OTR/intellij when falling back to disted libs

    val CppFallback: Name = new Name("MS-cpp-fallback")

    val ModuleLoads: Name = new Name("MS-module-loads")
    val PremainClass = new Name("Premain-Class")

    // Backward compatibility
    val JNIPath: Name = new Name("MS-native-library-path")
    val PreloadPath: Name = new Name("MS-preload-path")
  }

  def defaultManifest(
      meta: String,
      project: String,
      version: String,
      stratoVersion: String,
      obtVersion: String
  ): Map[String, String] =
    Map(
      "Manifest-Version" -> "1.0",
      "Implementation-Title" -> s"$meta/$project",
      "Implementation-Version" -> version,
      "Specification-Version" -> version,
      "Stratosphere-Version" -> stratoVersion,
      "Buildtool-Version" -> obtVersion,
      "Implementation-Vendor" -> "Morgan Stanley"
    ) ++ sys.env.get("GIT_COMMIT").map("Build-Commit" -> _) // set by Jenkins...

  // Note: this only supports creation of jars in the default filesystem (eg. not in JimFS). Existing jars can
  // be read from JimFS however.
  def jarFileSystem(jarFile: JarAsset, create: Boolean = false): FileSystem = {
    import scala.jdk.CollectionConverters._
    if (create)
      FileSystems.newFileSystem(URI.create("jar:file:" + jarFile.path.toUri.getPath), Map("create" -> "true").asJava)
    else
      FileSystems.newFileSystem(jarFile.path, null.asInstanceOf[ClassLoader])
  }

  def load(m: jar.Manifest, key: Name): Option[String] = m.getMainAttributes.getValue(key) match {
    case null | "" => None
    case str       => Some(str)
  }

  def load(m: jar.Manifest, key: Name, sep: String): Seq[String] =
    load(m, key).map(_.split(sep).map(_.trim).to(Seq)).getOrElse(Nil)
}
