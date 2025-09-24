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
package optimus.rest.bitbucket

import optimus.rest.RestApi
import spray.json.JsonReader

import scala.annotation.tailrec

trait RecursiveQuerier extends RestApi {
  private val generalPageSize: Int = 100

  protected def queryPaged[Result, Collection <: Paged[Result]: JsonReader](
      url: String,
      maxResults: Option[Int] = None,
      generalPageSize: Int = generalPageSize): Seq[Result] = {
    require(
      !url.contains("start=") && !url.contains("limit="),
      s"Url cannot contain 'limit=' nor 'start=' as this method sets those values. Got: $url")

    @tailrec def query(start: Int, acc: Seq[Result]): Seq[Result] = {
      val separator = if (url.contains("?")) "&" else "?"
      val response = get[Collection](s"$url${separator}limit=$generalPageSize&start=$start")

      if (isReallyLastPage(response)) {
        acc ++ response.values
      } else {
        val resultFetched = acc.size + response.values.size
        (response.nextPageStart, maxResults) match {
          // only query more if there's another page AND we're not over maxResults already
          case (Some(nextPageStart), maxResults) if maxResults.forall(_ >= resultFetched) =>
            query(nextPageStart, acc ++ response.values)
          // If we don't want more but there is more to have, cut off what we want
          case (Some(_), Some(maxResults)) =>
            val amountWanted = maxResults - acc.size
            acc ++ response.values.take(amountWanted)
          case _ =>
            acc ++ response.values
        }
      }
    }
    query(0, Seq.empty)
  }

  // Bitbucket API is broken and 'isLastPage' is not always set properly
  private def isReallyLastPage[Collection <: Paged[_]](coll: Collection): Boolean =
    coll.isLastPage || coll.values.isEmpty || coll.nextPageStart.isEmpty
}
