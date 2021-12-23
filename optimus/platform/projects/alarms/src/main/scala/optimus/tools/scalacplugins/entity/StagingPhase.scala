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
package optimus.tools.scalacplugins.entity

object StagingPhase {

  // we don't have whole phase list here
  // use "-Xshow-phases" to display all the compile phases
  object names {
    val parser = "parser" // scalac phase: parse source into ASTs, perform simple desugaring
    val typer = "typer"
    val superaccessors = "superaccessors" // scalac phase: parse source into ASTs, perform simple desugaring
    val pickler = "pickler"
    val refchecks = "refchecks"

    val optimus_staging = "optimus_staging"
    val optimus_annotator = "optimus_annotator"
    val optimus_standards = "optimus_standards"
    val optimus_forwarding = "optimus_forwarding"
    val optimus_rewrite = "optimus_rewrite"

    val namer = "namer" // scalac phase: resolve names, attach symbols to named trees
  }

  import names._

  // OptimusPhaseInfo(phaseName, runsAfter, runsBefore)
  // these three run one after each other, after parsing but before namer or adjustast
  val STAGING = OptimusPhaseInfo(optimus_staging, "remove program elements based on @staged conditions", parser, namer)
  val STANDARDS = OptimusPhaseInfo(
    optimus_standards,
    "fail when certain coding standards are violated",
    optimus_staging,
    optimus_annotator)
  val ANNOTATING = OptimusPhaseInfo(optimus_annotator, "add annotations to library symbols", optimus_standards, namer)
  val FORWARDING =
    OptimusPhaseInfo(optimus_forwarding, "rewrite references to @forwarder values", typer, superaccessors)
  val REWRITE = OptimusPhaseInfo(optimus_rewrite, "rewrite source files in-place during migration", pickler, refchecks)
}
