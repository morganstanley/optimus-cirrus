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
package optimus.buildtool.builders.postbuilders

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.config.ScopeId
import optimus.platform._

import scala.collection.immutable.Seq

/**
 * A set of actions to run after various stages of building. Does not build posts.
 */
trait PostBuilder {

  /**
   * Post-process artifacts for a given scope. Note that `artifacts` here deliberately isn't just the direct artifacts
   * for `id`. This is because some post-builders may need the full set of artifacts a scope depends on (eg. in
   * Installer, so that it can rename dependencies in jar files). If you only want the artifacts for `id` in this
   * method, consider extending [[FilteredPostBuilder]] instead.
   *
   * @param id
   *   a direct scope (ie. one that was explicitly built)
   */
  @async def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {}

  /**
   * Post-process artifacts for a set of transitive scopes. Note that `artifacts` here deliberately isn't just the
   * direct artifacts for `transitiveScopes`. This is because some post-builders may need the full set of artifacts a
   * scope depends on (eg. in Installer, so that it can rename dependencies in jar files). If you only want the
   * artifacts for `transitiveScopes` in this method, consider extending [[FilteredPostBuilder]] instead.
   *
   * @param transitiveScopes
   *   the set of transitive scopes (ie. ones that were not explicitly built)
   */
  @async def postProcessTransitiveArtifacts(transitiveScopes: Set[ScopeId], artifacts: Seq[Artifact]): Unit = {}

  /**
   * Post-process all artifacts. Note that all artifacts will also have been passed to [[postProcessScopeArtifacts]] and
   * [[postProcessTransitiveArtifacts]] previously, so in general if those methods have been implemented, this one will
   * not need to be.
   *
   * @param scopes
   *   the set of direct scopes (ie. ones that were explicitly built)
   * @param successful
   *   whether the whole build was successful (that is, all MessageArtifacts have no errors)
   */
  @async def postProcessArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact], successful: Boolean): Unit = {}

  /** Completes the build. A failure in this section is considered critical */
  def complete(successful: Boolean): Unit = ()

  /** Save state associated with the PostBuilder. A failure in this section is non-critical */
  def save(successful: Boolean): Unit = ()
}

object PostBuilder {

  /** Null object for [[PostBuilder]]s. */
  val zero: PostBuilder = new PostBuilder {}

  private class MergingPostBuilder(builders: Seq[PostBuilder]) extends PostBuilder {
    @async override def postProcessArtifacts(
        scopes: Set[ScopeId],
        artifacts: Seq[Artifact],
        successful: Boolean): Unit =
      builders.apar.foreach(_.postProcessArtifacts(scopes, artifacts, successful))
    @async override def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit =
      builders.apar.foreach(_.postProcessScopeArtifacts(id, artifacts))
    @async override def postProcessTransitiveArtifacts(scopes: Set[ScopeId], artifacts: Seq[Artifact]): Unit =
      builders.apar.foreach(_.postProcessTransitiveArtifacts(scopes, artifacts))
    override def complete(successful: Boolean): Unit = builders.foreach(_.complete(successful))
    override def save(successful: Boolean): Unit = builders.foreach(_.save(successful))
  }

  def merge(builders: Seq[PostBuilder]): PostBuilder = builders match {
    case Seq(b) => b
    case bs     => new MergingPostBuilder(bs)
  }
}

trait FilteredPostBuilder extends PostBuilder {

  @async override final def postProcessScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {
    val scopeArtifacts = artifacts.collect { case InternalArtifact(InternalArtifactId(`id`, _, _), a) =>
      a
    }
    postProcessFilteredScopeArtifacts(id, scopeArtifacts)
  }
  @async protected def postProcessFilteredScopeArtifacts(id: ScopeId, artifacts: Seq[Artifact]): Unit = {}

  @async override final def postProcessTransitiveArtifacts(
      transitiveScopes: Set[ScopeId],
      artifacts: Seq[Artifact]
  ): Unit = {
    val transitiveArtifacts = artifacts.collect {
      case InternalArtifact(InternalArtifactId(scopeId, _, _), a) if transitiveScopes.contains(scopeId) => (scopeId, a)
    }.toGroupedMap
    postProcessFilteredTransitiveArtifacts(transitiveArtifacts)
  }
  @async protected def postProcessFilteredTransitiveArtifacts(
      transitiveArtifacts: Map[ScopeId, collection.Seq[Artifact]]
  ): Unit = {}
}
