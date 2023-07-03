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
package optimus.platform.relational.pivot

import optimus.platform.relational.MapBasedDynamicObject

trait PivotRowFactory {
  def createPivotRow(m: Map[String, Any]): PivotRow
  def pivotRowClass: Class[_]
}

/**
 * Implement PivotRowFactory to get rid of inner class ctor issue which requires the instance of outer class when we use
 * reflection to create instance.
 */
abstract class PivotRow(val map: Map[String, Any]) extends MapBasedDynamicObject(map) with PivotRowFactory {
  override def toString = "PivotRow[data:" + this.data.toString + "]"
  override def pivotRowClass: Class[_] = this.getClass()
}

class DefaultPivotRow(override val map: Map[String, Any]) extends PivotRow(map) {
  override def createPivotRow(m: Map[String, Any]): PivotRow = new DefaultPivotRow(m)
}
