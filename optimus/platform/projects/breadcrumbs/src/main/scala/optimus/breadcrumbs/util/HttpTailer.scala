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
package optimus.breadcrumbs.util

import java.util.Timer
import java.util.TimerTask

import org.apache.http.HttpException
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.kohsuke.args4j.{Option => ArgOption}
import org.kohsuke.args4j.CmdLineParser
import org.slf4j.LoggerFactory

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

class HttpTailer(url: String, periodMs: Int, blockSize: Int, userCallback: Try[String] => Unit) {
  private val log = LoggerFactory.getLogger(this.getClass)

  // Regex to match Content-Range header value, e.g.
  //   Content-Range: 100-157/300
  // means the returned content is bytes 100 through 157 (inclusive), out of a known total of 300 bytes.
  val rangeRe: Regex = """.*(\d+)-(\d+)/(\d+).*""".r

  val client: CloseableHttpClient = HttpClients.createDefault()
  val get = new HttpGet(url)
  val timer = new Timer()

  def stop(): Unit = {
    timer.cancel()
  }

  processContentAndScheduleRangeFetch(-1, 0, "", probe = true)

  /**
   * Schedule http get for specific range:
   * @param i2
   *   Last byte offset previously read (-1 if never read)
   * @param n
   *   Known total available size
   * @param content
   *   Content read in previous fetch
   * @param probe
   *   True if this call is just to probe for total available size
   */
  private def processContentAndScheduleRangeFetch(i2: Long, n: Long, content: String, probe: Boolean = false): Unit = {
    log.debug(s"scheduleRangeFetch(i2=$i2, n=$n, content-length=${content.length}, probe=$probe)")
    if (content.length > 0) {
      userCallback(Success(content))
    }

    // Set new Range header
    get.removeHeaders("Range")
    // Read from next byte after content previously read, unless the total content has grown such that this
    // would exceed the maximum blocksize, in which case, we're willing to drop content.
    val i1Req = Math.max(i2 + 1, n - blockSize)
    val i2Req = i1Req + blockSize
    log.debug(s"Requesting bytes=$i1Req-$i2Req")
    get.setHeader("Range", s"bytes=$i1Req-$i2Req")

    // Create task to perform the next fetch asynchronously.
    val task = new TimerTask {
      override def run(): Unit = {
        client.execute(
          get,
          (response: HttpResponse) => {
            try {
              val code = response.getStatusLine.getStatusCode
              log.debug(s"Got status code $code")
              // 404: No content available.  Assume it will eventually be, and schedule a new probe.
              if (code == 404)
                processContentAndScheduleRangeFetch(-1, 0, "", probe = true)
              // 416: "Range request not satisfied", i.e. there's no new content, so schedule another try.
              else if (code == 416)
                processContentAndScheduleRangeFetch(i2, n, "")
              // Lacking any content length is a hard error; if length is zero, that's equivalent to an unsatisfied
              // range request.
              else if (
                response
                  .getHeaders("Content-Length")
                  .headOption
                  .fold {
                    throw new HttpException("Missing Content-Length")
                  }(_.getValue.toInt == 0)
              )
                processContentAndScheduleRangeFetch(i2, n, "") // No content; keep trying.
              else {
                // The only acceptable response codes a this point are in the 200 range.
                if (code < 200 || code >= 300)
                  throw new HttpException(s"Unexpected status code $code")
                val crHeaders = response.getHeaders("Content-Range").toSeq
                if (crHeaders.size != 1)
                  throw new HttpException(s"Number of Content-Range headers = ${crHeaders.size}")
                val crVal = crHeaders.head.getValue
                log.debug(s"Content-Range: $crVal")
                // Extract header values: bytes i1 through i2 (inclusive) out of total so far of n
                val (i1: Long, i2: Long, n: Long) = rangeRe
                  .unapplySeq(crVal)
                  .flatMap { vals =>
                    Try(vals.map(_.toLong)).toOption.find(_.length == 3).map(l => (l(0), l(1), l(2)))
                  }
                  .getOrElse {
                    throw new HttpException(s"Malformed Content-Range: $crVal")
                  }
                // If we were just probing, schedule a real fetch.  The i2<i1 case should never happen...
                if (probe || i2 < i1) {
                  processContentAndScheduleRangeFetch(i2, n, "")
                }
                // Got some content!
                else {
                  val e = response.getEntity
                  if (e == null) throw new HttpException("No response entity")
                  val content = EntityUtils.toString(e)
                  processContentAndScheduleRangeFetch(i2, n, content)
                }
              }
            } catch {
              case t: Throwable =>
                try {
                  client.close()
                } catch {
                  case t: Throwable =>
                    log.warn(s"Got exception closing client: $t")
                }
                userCallback(Failure(t))
            }
          }
        )
      }
    }
    try {
      timer.schedule(task, periodMs)
    } catch {
      case _: IllegalStateException =>
        log.debug("Timer seems to have been cancelled")
    }
  }
}

class HttpTailerArgs {
  @ArgOption(name = "--url")
  var url: String = ""

  @ArgOption(name = "delay")
  var delayMs: Int = 5000

  @ArgOption(name = "blocksize")
  var blockSizeBytes = 1000

}
object HttpTailerCli extends App {

  val cli = new HttpTailerArgs
  val parser = new CmdLineParser(cli)
  parser.parseArgument(args: _*)
  import cli._

  val tailer = new HttpTailer(
    url,
    delayMs,
    blockSizeBytes,
    {
      case Success(s) =>
        // If you complain about this line during review, you haven't been paying attention.
        // Printing the result to stdout directly.
        print(s)
      case Failure(e) =>
        println /* deliberately printing to stdout in CLI app */ (s"Failure: $e")
    }
  )

}
