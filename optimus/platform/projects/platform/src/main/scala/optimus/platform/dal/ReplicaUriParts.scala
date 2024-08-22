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
package optimus.platform.dal

import java.net.URI

private[optimus] final case class ReplicaUriParts(
    replica: String,
    writer: Option[String],
    pubSub: Option[String],
    acc: Option[String],
    prc: Option[String],
    loadbalance: Option[String],
    messagesBrk: Option[String],
    secureCon: Option[String]
) {
  def toReplicaUri: String = {
    val queryParams = List(
      writer.map(m => s"writer=$m"),
      pubSub.map(p => s"pubsub=$p"),
      acc.map(a => s"acc=$a"),
      prc.map(p => s"prcsk=$p"),
      loadbalance.map(lb => s"loadbalance=$lb"),
      messagesBrk.map(m => s"msgsbrk=$m"),
      secureCon.map(sc => s"secureTransport=$sc")
    ).flatten
    val queryStr = if (queryParams.nonEmpty) queryParams.mkString("&") else null
    val uri = new URI(DSIURIScheme.REPLICA, replica, null, queryStr, null)
    uri.toString
  }
}

private[optimus] object ReplicaUriParts {
  def apply(uri: URI): ReplicaUriParts = {
    require(
      uri.getScheme == DSIURIScheme.REPLICA,
      s"URI scheme should be ${DSIURIScheme.REPLICA}, but was ${uri.getScheme}")
    val queryMap = DSIURIScheme.getQueryMap(uri)
    ReplicaUriParts(
      replica = uri.getAuthority,
      writer = queryMap.get("writer"),
      pubSub = queryMap.get("pubsub"),
      acc = queryMap.get("acc"),
      prc = queryMap.get("prcsk"),
      loadbalance = queryMap.get("loadbalance"),
      messagesBrk = queryMap.get("msgsbrk"),
      secureCon = queryMap.get("secureTransport")
    )
  }
}
