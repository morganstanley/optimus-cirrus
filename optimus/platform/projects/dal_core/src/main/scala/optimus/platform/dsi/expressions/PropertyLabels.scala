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
package optimus.platform.dsi.expressions

private[optimus] object PropertyLabels {
  val VersionedRef = "dal$versionRef"
  val EntityRef = "dal$entityRef"
  val StorageTxTime = "dal$storageInfo.txTime"
  val ChildRef = "child_ref"
  val ParentPropName = "parent_prop_name"
  val ParentRef = "parent_ref"
  val Rectangle = "rectangle"
}
