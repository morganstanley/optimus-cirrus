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
package optimus.platform.util

import scala.tools.nsc.Settings

object ScalaCompilerConfigurer {
  def updateSettingsForVersion(settings: Settings) = {
    // these are required for Scala 2.13

    // likely needs further review to ensure synchronized with Scalac settings in workspace.obt

    // without this Stargazer will fail with errors like
    // Compile ERROR type mismatch;
    // found : Seq[optimus.platform.Tweak] (in scala.collection)
    // required: Seq[optimus.platform.Tweak] (in scala.collection.immutable)

    // settings.imports doesn't exist in 2.12
    settings.imports.value ++= Seq("java.lang", "scala", "scala.Predef", "optimus.scala212.DefaultSeq")

    // without this Stargazer will fail with errors like
    // Compile ERROR postfix operator toSet needs to be enabled
    // by making the implicit value scala.language.postfixOps visible.
    // This can be achieved by adding the import clause 'import scala.language.postfixOps'
    // or by setting the compiler option -language:postfixOps.
    // See the Scaladoc for value scala.language.postfixOps for a discussion
    // why the feature needs to be explicitly enabled.
    // sync with workspace.obt where these are all set
    // -language:dynamics,postfixOps,reflectiveCalls,implicitConversions,higherKinds,existentials
    settings.language.enable(settings.languageFeatures.dynamics)
    settings.language.enable(settings.languageFeatures.postfixOps)
    settings.language.enable(settings.languageFeatures.reflectiveCalls)
    settings.language.enable(settings.languageFeatures.implicitConversions)
    settings.language.enable(settings.languageFeatures.higherKinds)
    settings.language.enable(settings.languageFeatures.existentials)
  }
}
