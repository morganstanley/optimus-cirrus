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
package optimus.profiler

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.lang.management._
import java.text.SimpleDateFormat
import java.util.Date
import javax.management._
import javax.management.remote._
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.CacheStats
import optimus.graph.GraphMBean
import optimus.graph.Settings
import optimus.graph.diagnostics.pgo.Profiler
import optimus.platform_profiler.config.StaticConfig

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

object QueryGraph extends App {

  val dir = args(0)
  val resultsDir = args(1)
  val files = new File(args(0))
  val fileNames = files.list()

  val estCacheSizes = new ArrayBuffer[Long]
  val finalMemSizes = new ArrayBuffer[Long]
  val allTags = new ArrayBuffer[PNodeTaskInfo]
  val cStats = new ArrayBuffer[CacheStats]
  var readServers = 0

  if (fileNames.nonEmpty) {

    try {
      import scala.concurrent._
      import scala.concurrent.ExecutionContext.Implicits.global
      val futures: List[Future[AnyVal]] = for (file <- fileNames.toList) yield Future {
        println("Trying: " + file)
        var jmxc: JMXConnector = null

        try {
          val parts = file.split("_")
          val graphmbx = Settings.getGraphObjectName
          val url = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://${parts(0)}:${parts(1)}/jmxrmi")
          jmxc = JMXConnectorFactory.connect(url, null);
          val mbsc = jmxc.getMBeanServerConnection()

          val memory =
            ManagementFactory.newPlatformMXBeanProxy(mbsc, ManagementFactory.MEMORY_MXBEAN_NAME, classOf[MemoryMXBean])
          val graph = JMX.newMBeanProxy(mbsc, graphmbx, classOf[GraphMBean])
          val cstats = graph.profileCacheMemory(precise = true, StaticConfig.string("profileClsOfInterest"))
          val td = graph.collectProfile(resetAfter = false, alsoClearCache = false)

          println("Cache total size: " + cstats.totalCacheSize)
          println("Root reached objects: " + cstats.countOfRootReachableObjectsOfInterest)
          println("Cache densities: " + cstats.densities.mkString(", "))
          println("GCs: " + cstats.gcInfos.mkString(" "))
          // println("Final memory (%s Mb), Est cache memory (%s Mb)".format(finalHeap, startHeap - finalHeap))

          allTags.synchronized {
            //          estCacheSizes.append(startHeap - finalHeap)
            //          finalMemSizes.append(finalHeap)
            cStats.append(cstats)
            allTags.append(td: _*)
            readServers += 1
          }
        } catch {
          case ex: IOException => {
            println(file + ": " + ex.getMessage)
            val fileo = new File(dir + File.separator + file)
            fileo.delete();
          }
          case ex: Throwable =>
            println(file + ": " + ex)
        } finally {
          if ((jmxc ne null))
            jmxc.close()
          jmxc = null
        }
      }
      Await.result(Future.sequence(futures), Duration.Inf)
    } catch {
      case ex: Throwable =>
        println("loop: " + ex)
    }

    println("_________________________________________")
    println("Queried:   " + fileNames.length)
    println("Collected: " + readServers)
    if (readServers > 0) {
      //      println("Avg. Cache Size:  " + estCacheSizes.sum / estCacheSizes.size)
      //      println("Avg. Final Size:  " + finalMemSizes.sum / finalMemSizes.size)
      println("Avg. Time in GCs: " + cStats.foldLeft(0L)(_ + _.gcTime) / cStats.size)
      println("Avg. # of GCs:    " + cStats.foldLeft(0L)(_ + _.gcCount) / cStats.size)
      println("Avg. # of Clr$:   " + cStats.foldLeft(0L)(_ + _.clearAllCount) / cStats.size)

      val grouped: Seq[PNodeTaskInfo] = Profiler.combineTraces(allTags)
      val sorted = grouped.sortBy(_.fullName)

      // Profiler.writeProfileData(new FileWriter("all_result.csv"), allTags)
      val stamp = new SimpleDateFormat("yyyyMMdd_hhmmss").format(new Date())
      Profiler.writeProfileData(new FileWriter(new File(resultsDir, "grouped_result_" + stamp + ".csv")), sorted)
      val file = new File(resultsDir, "optimus_" + stamp + ".config")
      Profiler.autoGenerateConfig(file.getName, sorted)
    }
  } else {
    println("No server files found")
  }
}
