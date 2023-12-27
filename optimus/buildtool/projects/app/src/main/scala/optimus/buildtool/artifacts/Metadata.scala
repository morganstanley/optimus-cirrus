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
package optimus.buildtool.artifacts

import optimus.buildtool.files.RelativePath

import scala.collection.immutable.Seq

object CachedMetadata {
  val MetadataFile = RelativePath("metadata.json")
}

trait CachedMetadata

final case class MessagesMetadata(messages: Seq[CompilationMessage], hasErrors: Boolean) extends CachedMetadata

final case class CppMetadata(
    osVersion: String,
    releaseFile: Option[RelativePath],
    debugFile: Option[RelativePath],
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata

final case class GeneratedSourceMetadata(
    generatorName: String,
    sourcePath: RelativePath,
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata

final case class ProcessorMetadata(
    processorName: String,
    messages: Seq[CompilationMessage],
    hasErrors: Boolean
) extends CachedMetadata
