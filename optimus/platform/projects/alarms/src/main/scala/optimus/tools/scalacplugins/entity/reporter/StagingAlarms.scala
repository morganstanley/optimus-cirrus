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
package optimus.tools.scalacplugins.entity.reporter

import optimus.tools.scalacplugins.entity.StagingPhase

object CodeStyleErrors extends OptimusErrorsBase with OptimusPluginAlarmHelper {
  val RETURN_STATEMENT =
    error0(20011, StagingPhase.STANDARDS, s"${OptimusAlarms.NewTag} Return statement is not needed here")
}

object StagingErrors extends OptimusErrorsBase with OptimusPluginAlarmHelper {

  // staging phase errors
  val STAGING_DEPRECATED = error1(
    20001,
    StagingPhase.STAGING,
    "Staging marker %s is deprecated and will be removed.  Please remove staging block.")
  val INVALID_STAGING_PREDICATE = error1(20002, StagingPhase.STAGING, "invalid staging predicate: %s")
  val MUTIPLE_STAGING_OBJECT =
    error0(20003, StagingPhase.STAGING, "staging imports only support one import for each object")

  // code-standards phase errors
  val NO_PACKAGE_OBJECT_IMPORT =
    error0(
      20010,
      StagingPhase.STANDARDS,
      "Do not import paths containing explicit package objects (just remove .`package`)")
}

object StagingNonErrorMessages extends OptimusNonErrorMessagesBase with OptimusPluginAlarmHelper {
  // staging phase warnings
  val UNKNOWN_STAGING_MARKER = warning1(10004, StagingPhase.STAGING, "Unknown staging marker type: %s")

  // code-standards phase warnings
  val UNTYPED_IMPLICIT = preIgnore(
    warningOptional1(
      10005,
      StagingPhase.STANDARDS,
      "Public implicit methods and classes should have explicit type: %s"))
}
