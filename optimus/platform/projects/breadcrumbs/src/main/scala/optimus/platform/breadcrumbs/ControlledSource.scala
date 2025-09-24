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
package optimus.platform.breadcrumbs

trait ControlledSource {
  // This trait exists so that we can make the following method private to optimus.platform.
  // Really, optimus.breadcrumbs ought to be in package platform, but that's a pretty big PR.
  private[platform] def filterable: Boolean = true
  final def isFilterable: Boolean = filterable

}
