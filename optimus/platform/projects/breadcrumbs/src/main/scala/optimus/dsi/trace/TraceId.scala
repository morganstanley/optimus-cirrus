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
package optimus.dsi.trace

import optimus.breadcrumbs.ChainedID

final case class TraceId(requestId: String, chainedId: ChainedID = ChainedID.root) {
  def childId = chainedId.child
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case obj: TraceId => obj.requestId == requestId
      case _            => false
    }
  }
  override def hashCode(): Int = requestId.hashCode
}

final case class DalRequestId(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

private[optimus] object DalRequestId {
  // ideally shouldn't be on the client classpath but some existing client APIs take a dependency to be unpicked later
  val Admin: DalRequestId = DalRequestId("3c61646d-696e-3e00-0000-000000000000")
}
