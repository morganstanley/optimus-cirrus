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
package optimus.platform

import optimus.platform.internal.StagingMarkersBase

/**
 * Via the Staging feature, API users can key the code execution based on the presence of a particular bit of API
 * (method, class, field,...) But what if the staging required is on a particular implementation fix which has no
 * visible API impact?
 *
 * The Optimus Staging feature needs something tractable to for the predicate evaluation.
 *
 * Thus Optimus developers can leave "staging markers" in this class to indicate the presence of a certain fix.
 *
 * Markers will be removed after a certain number of weeks.
 *
 * Markers can take different states:
 *   - Implemented : the feature/fix represented by this marker is implemented in this version of the codebase
 *   - NotImplemented : the feature/fix represented by this marker is NOT implemented in this version of the codebase
 *     (this is also the assumed state if the marker doesn't exist)
 *   - MarkerToBeDeleted : this is a special state that is used to "break" anyone still using the marker. Before
 *     deleting the marker, a release (or edge CI) with this state must be done. The optimus staging functionality will
 *     break any code still using the marker when it is in MarkerToBeDeleted stage. If none of the user code breaks, the
 *     marker can then be deleted.
 *
 * Sample client code usage: if
 * (hasStagingMarker("optimus.platform.StagingMarkers.RELATIONAL_DO_NOT_CONVERT_NODE_METHODS"): @staged) {....
 *
 * See further documentation in the user manual: http://Optimus/UserManual#Staging
 *
 * Since this file is used during Scala version upgrades, it is not used at all times. Do not delete it until we have a
 * new upgrade strategy or we stop upgrading.
 */
class StagingMarkers extends StagingMarkersBase {
  /*
   * Standard for adding markers:
   * 1) use a descriptive name
   * 2) in comment, put the date and the Changeset OR changelist OR Jira link for this particular feature/fix.
   *
   */
  def RELATIONAL_DO_NOT_CONVERT_NODE_METHODS = MarkerToBeDeleted // CS-29938 : Date May-2-2012
  def SCALA_210 = Implemented // Differentiate between Scala 2.9 and 2.10 builds of Optimus
  def EVENT_REFERENCE = Implemented
  def GENERIC_COLLECTION_PICKLER = MarkerToBeDeleted
  def RUNTIME_ENV_CREATION_BOOLEAN = Implemented
}
