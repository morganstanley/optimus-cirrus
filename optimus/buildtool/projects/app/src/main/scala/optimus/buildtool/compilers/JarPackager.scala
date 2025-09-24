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

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.InternalClassFileArtifact
import optimus.buildtool.config.NamingConventions.ContentType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing.hashFileOrDirectoryContent
import optimus.platform._

@entity private[buildtool] class JarPackager {
  import JarPackager._

  @node def artifact(scopeId: ScopeId, inputs: NodeFunction0[Inputs]): Option[InternalClassFileArtifact] = {
    val resolvedInputs = inputs()
    import resolvedInputs._
    if (content.nonEmpty) {
      ObtTrace.traceTask(scopeId, task) {
        AssetUtils.atomicallyWrite(jarPath) { tempOut =>
          // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
          val tempJar = new ConsistentlyHashedJarOutputStream(JarAsset(tempOut), None, compressed = true)
          try {
            content.foreach { case (file, content) =>
              if (tokens.isEmpty || content.tpe == ContentType.Binary)
                tempJar.copyInFile(content.contentAsInputStream, file.sourceFolderToFilePath)
              else {
                val newContent = tokens.foldLeft(content.utf8ContentAsString) { case (c, (key, value)) =>
                  c.replace(s"@$key@", value)
                }
                tempJar.writeFile(newContent, file.sourceFolderToFilePath)
              }
            }
          } finally tempJar.close()
        }

        Some(
          InternalClassFileArtifact.create(
            InternalArtifactId(scopeId, artifactType, None),
            jarPath,
            hashFileOrDirectoryContent(jarPath),
            incremental = false,
            containsPlugin = containsPlugin,
            containsAgent = containsAgent,
            containsOrUsedByMacros = containsOrUsedByMacros
          ))
      }
    } else {
      None
    }
  }
}

private[buildtool] object JarPackager {
  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  // This is the node through which packaging is initiated. It's very important that we don't lose these from
  // cache (while they are still running at least) because that can result in repackaging of the same scope
  // due to a (potentially large) race between checking if the output artifacts are on disk and actually writing
  // them there after packaging completes.
  artifact.setCustomCache(reallyBigCache)

  final case class Inputs(
      task: CategoryTrace,
      artifactType: ArtifactType,
      jarPath: JarAsset,
      content: Map[SourceUnitId, HashedContent],
      tokens: Map[String, String],
      containsPlugin: Boolean,
      containsAgent: Boolean,
      containsOrUsedByMacros: Boolean
  )
}
