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
package optimus.graph.utils

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.file.Paths
import java.util.Properties

import com.sun.tools.attach.VirtualMachine
import javax.management.remote.JMXConnector
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import sun.tools.attach.HotSpotVirtualMachine

object VirtualMachineUtils {
  private val log: Logger = getLogger(this.getClass)
  lazy val self: Option[VirtualMachineUtils] = {
    import com.sun.tools.attach.VirtualMachine
    System.setProperty("jdk.attach.allowAttachSelf", "true")

    try {
      val pid = ProcessHandle.current.pid.toString
      Some(new VirtualMachineUtils(VirtualMachine.attach(pid)))
    } catch {
      case e: Exception =>
        log.error(s"unable to attach to our own VirtualMachine", e)
        None
    }
  }
}
class VirtualMachineUtils(vm: VirtualMachine) {
  import VirtualMachineUtils.log
  lazy val hsvm = vm.asInstanceOf[HotSpotVirtualMachine]

  private def readFully(inputStream: InputStream): Seq[String] = {
    val is = new BufferedReader(new InputStreamReader(inputStream))
    var line = is.readLine()
    val builder = List.newBuilder[String]
    while (line ne null) {
      builder += line
      line = is.readLine()
    }
    builder.result()
  }
  private def bool(value: Boolean, textIfTrue: String) = {
    if (value) textIfTrue else ""
  }
  private def notNull(value: String, textIfNotNull: String) = {
    if (value eq null) "" else s"$textIfNotNull=$value"
  }
  object jmx {

    lazy val remote = {
      val url = vm.startLocalManagementAgent()
      val connector: JMXConnector = JMXConnectorFactory.connect(new JMXServiceURL(url))
      connector.getMBeanServerConnection
    }
  }

  /**
   * simulates SIG_HUP - get the threads
   * @see
   *   Jcmd.Thread_print for other options
   * @return
   */
  def dumpThread: Seq[String] = {
    readFully(hsvm.remoteDataDump())
  }
  lazy val getSystemProperties: Properties = {
    vm.getSystemProperties
  }
  def getAgentProperties: Properties = {
    vm.getAgentProperties
  }
  object Properties {
    lazy val userDir: String = getSystemProperties.getProperty("user.dir")
    lazy val userDirPath = Paths.get(userDir)
  }
  object Jcmd {

    /**
     * lists the commands available. For help on a specific command supply the command/subcommand as args For more
     * information about a specific command use jCmdHelp(<command>)
     */
    def help(args: String = ""): Seq[String] = {
      execute((s"help $args"))
    }
    def execute(args: String): Seq[String] = {
      log.debug(s"execute JCMD command : $args")
      val result = readFully(hsvm.executeJCmd(args))
      if (log.isDebugEnabled()) {
        log.debug(s"Start response to JCMD command")
        result.foreach { line =>
          log.debug(s"response to JCMD command : $line")
        }
        log.debug(s"End response to JCMD command")
      }

      result
    }

    object Thread {

      /**
       * get all threads with stacktraces. JCMD help -
       * --- Thread.print Print all threads with stacktraces.
       *
       * Impact: Medium: Depends on the number of threads.
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : Thread.print [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -l : [optional] print java.util.concurrent locks (BOOLEAN, false)
       * -e : [optional] print extended thread information (BOOLEAN, false)
       *
       * @param locks
       *   print java.util.concurrent locks. default false
       * @param extended
       *   print extended thread information. default false
       * @return
       */
      def print(locks: Boolean = false, extended: Boolean = false): Seq[String] = {
        execute(s"Thread.print ${bool(locks, "-l")} ${bool(extended, "-e")}")
      }
    }
    object GC {

      /**
       * request a GC. JCMD help -
       * --- GC.run Call java.lang.System.gc().
       *
       * Impact: Medium: Depends on Java heap size and content.
       *
       * Syntax: GC.run
       */
      def run: Seq[String] = {
        execute("GC.run")
      }

      /**
       * request finalization. JCMD help -
       * --- GC.run_finalization Call java.lang.System.runFinalization().
       *
       * Impact: Medium: Depends on Java content.
       *
       * Syntax: GC.run_finalization
       */
      def run_finalization: Seq[String] = {
        execute("GC.run_finalization")
      }

      /**
       * Provide statistics about the Java heap usage. JCMD help -
       * --- GC.class_histogram Provide statistics about the Java heap usage.
       *
       * Impact: High: Depends on Java heap size and content.
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : GC.class_histogram [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -all : [optional] Inspect all objects, including unreachable objects (BOOLEAN, false)
       *
       * @param all
       *   Inspect all objects, including unreachable objects. default false
       */
      def class_histogram(all: Boolean = false): Seq[String] = {
        execute(s"GC.class_histogram ${bool(all, "-all")}")
      }

      /**
       * Provide statistics about Java class meta data. JCMD help -
       * --- GC.class_stats Provide statistics about Java class meta data.
       *
       * Impact: High: Depends on Java heap size and content.
       *
       * Syntax : GC.class_stats [options] [<columns>]
       *
       * Arguments: columns : [optional] Comma-separated list of all the columns to show. If not specified, the
       * following columns are shown:
       * InstBytes,KlassBytes,CpAll,annotations,MethodCount,Bytecodes,MethodAll,ROAll,RWAll,Total (STRING, no default
       * value)
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -all : [optional] Show all columns (BOOLEAN, false)
       * -csv : [optional] Print in CSV (comma-separated values) format for spreadsheets (BOOLEAN, false)
       * -help : [optional] Show meaning of all the columns (BOOLEAN, false)
       *
       * @param all
       *   : Show all columns. default false
       * @param csv
       *   : Print in CSV (comma-separated values) format for spreadsheets. default false
       * @param help
       *   : Show meaning of all the columns. default false
       */
      def class_stats(all: Boolean = false, csv: Boolean = false, help: Boolean = false): Seq[String] = {
        execute(s"GC.class_stats ${bool(all, "-all")}")
      }

      /**
       * Provide information about Java finalization queue. JCMD help -
       * --- GC.finalizer_info Provide information about Java finalization queue.
       *
       * Impact: Medium
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax: GC.finalizer_info
       */
      def finalizer_info: Seq[String] = {
        execute(s"GC.finalizer_info ")
      }

      /**
       * Generate a HPROF format dump of the Java heap. Note the heap dump is not returned, just the status message JCMD
       * help -
       * --- GC.heap_dump Generate a HPROF format dump of the Java heap.
       *
       * Impact: High: Depends on Java heap size and content. Request a full GC unless the '-all' option is specified.
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : GC.heap_dump [options] <filename>
       *
       * Arguments: filename : Name of the dump file (STRING, no default value)
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -all : [optional] Dump all objects, including unreachable objects (BOOLEAN, false)
       *
       * @param filename
       *   Name of the dump file
       * @param all
       *   Dump all objects, including unreachable objects. default false
       */
      def heap_dump(filename: String, all: Boolean = false): Seq[String] = {
        execute(s"GC.heap_dump $filename ${bool(all, "-all")}")
      }

      /**
       * Provide statistics about Java class meta data. JCMD help -
       * --- GC.heap_info Provide generic Java heap information.
       *
       * Impact: Medium
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax: GC.heap_info
       */
      def heap_info: Seq[String] = {
        execute(s"GC.heap_info ")
      }
    }
    object JFR {

      /**
       * Configure JFR. JCMD help -
       * --- JFR.configure Configure JFR
       *
       * Impact: Low
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : JFR.configure [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax) repositorypath : [optional] Path
       * to repository,.e.g \"My Repository\" (STRING, no default value) dumppath : [optional] Path to dump,.e.g \"My
       * Dump path\" (STRING, no default value) stackdepth : [optional] Stack Depth (JLONG, 64) globalbuffercount :
       * [optional] Number of global buffers, (JLONG, 32) globalbuffersize : [optional] Size of a global buffers,
       * (JLONG, 524288) thread_buffer_size : [optional] Size of a thread buffer (JLONG, 8192) memorysize : [optional]
       * Overall memory size, (JLONG, 16777216) maxchunksize : [optional] Size of an individual disk chunk (JLONG,
       * 12582912) samplethreads : [optional] Activate Thread sampling (BOOLEAN, true)
       *
       * @param args
       *   : Options: (options must be specified using the <key> or <key>=<value> syntax)
       */
      def configure(args: String): Seq[String] = {
        val cmd = s"JFR.configure $args"
        log.info(s"CMD = $cmd")
        execute(cmd)
      }

      /**
       * Copies contents of a JFR recording to file. Either the name or the recording id must be specified. JCMD help -
       * --- JFR.dump Copies contents of a JFR recording to file. Either the name or the recording id must be specified.
       *
       * Impact: Low
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : JFR.dump [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax) name : [optional] Recording name,
       * e.g. \"My Recording\" (STRING, no default value) filename : [optional] Copy recording data to file, e.g.
       * "C:\\Users\\user\\My Recording.jfr" (STRING, no default value) maxage : [optional] Maximum duration to dump, in
       * (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit (NANOTIME, 0) maxsize : [optional]
       * Maximum amount of bytes to dump, in (M)B or (G)B, e.g. 500M, or 0 for no limit (MEMORY SIZE, 0) begin :
       * [optional] Point in time to dump data from, e.g. 09:00, 21:35:00, 2018-06-03T18:12:56.827Z,
       * 2018-06-03T20:13:46.832, -10m, -3h, or -1d (STRING, no default value) end : [optional] Point in time to dump
       * data to, e.g. 09:00, 21:35:00, 2018-06-03T18:12:56.827Z, 2018-06-03T20:13:46.832, -10m, -3h, or -1d (STRING, no
       * default value) path-to-gc-roots : [optional] Collect path to GC roots (BOOLEAN, false)
       *
       * @param name
       *   : Recording name, e.g. \"My Recording\"
       * @param otherArgs
       *   Options: (options must be specified using the <key> or <key>=<value> syntax) name : [optional] Recording
       *   name, e.g. \"My Recording\" (STRING, no default value) filename : [optional] Copy recording data to file,
       *   e.g. "C:\\Users\\user\\My Recording.jfr" (STRING, no default value) maxage : [optional] Maximum duration to
       *   dump, in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit (NANOTIME, 0) maxsize :
       *   [optional] Maximum amount of bytes to dump, in (M)B or (G)B, e.g. 500M, or 0 for no limit (MEMORY SIZE, 0)
       *   begin : [optional] Point in time to dump data from, e.g. 09:00, 21:35:00, 2018-06-03T18:12:56.827Z,
       *   2018-06-03T20:13:46.832, -10m, -3h, or -1d (STRING, no default value) end : [optional] Point in time to dump
       *   data to, e.g. 09:00, 21:35:00, 2018-06-03T18:12:56.827Z, 2018-06-03T20:13:46.832, -10m, -3h, or -1d (STRING,
       *   no default value) path-to-gc-roots : [optional] Collect path to GC roots (BOOLEAN, false)
       */
      def dump(name: String, otherArgs: String): Seq[String] = {
        execute(s"JFR.dump name=$name $otherArgs")
      }

      /**
       * Starts a new JFR recording JCMD help -
       * --- Starts a new JFR recording
       *
       * Impact: Medium: Depending on the settings for a recording, the impact can range from low to high.
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : JFR.start [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax) name : [optional] Name that can be
       * used to identify recording, e.g. "MyRecording" (STRING, no default value) settings : [optional] Settings
       * file(s), e.g. profile or default. See JRE_HOME/lib/jfr (STRING SET, no default value) delay : [optional] Delay
       * recording start with (s)econds, (m)inutes), (h)ours), or (d)ays, e.g. 5h. (NANOTIME, 0) duration : [optional]
       * Duration of recording in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 300s. (NANOTIME, 0) disk : [optional]
       * Recording should be persisted to disk (BOOLEAN, no default value) filename : [optional] Resulting recording
       * filename, e.g. "C:\\Users\\user\\MyRecording.jfr" (STRING, no default value) maxage : [optional] Maximum time
       * to keep recorded data (on disk) in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit
       * (NANOTIME, 0) maxsize : [optional] Maximum amount of bytes to keep (on disk) in (k)B, (M)B or (G)B, e.g. 500M,
       * or 0 for no limit (MEMORY SIZE, 0) dumponexit : [optional] Dump running recording when JVM shuts down (BOOLEAN,
       * no default value) path-to-gc-roots : [optional] Collect path to GC roots (BOOLEAN, false)
       *
       * @param name
       *   : Recording text,.e.g "MyRecording"
       * @param settings
       *   : [optional] Settings file(s), e.g. profile or default. See JRE_HOME/lib/jfr
       * @param otherArgs
       *   Options: (options must be specified using the <key> or <key>=<value> syntax) name : [optional] Name that can
       *   be used to identify recording, e.g. "MyRecording" (STRING, no default value) settings : [optional] Settings
       *   file(s), e.g. profile or default. See JRE_HOME/lib/jfr (STRING SET, no default value) delay : [optional]
       *   Delay recording start with (s)econds, (m)inutes), (h)ours), or (d)ays, e.g. 5h. (NANOTIME, 0) duration :
       *   [optional] Duration of recording in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 300s. (NANOTIME, 0) disk :
       *   [optional] Recording should be persisted to disk (BOOLEAN, no default value) filename : [optional] Resulting
       *   recording filename, e.g. "C:\\Users\\user\\MyRecording.jfr" (STRING, no default value) maxage : [optional]
       *   Maximum time to keep recorded data (on disk) in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for
       *   no limit (NANOTIME, 0) maxsize : [optional] Maximum amount of bytes to keep (on disk) in (k)B, (M)B or (G)B,
       *   e.g. 500M, or 0 for no limit (MEMORY SIZE, 0) dumponexit : [optional] Dump running recording when JVM shuts
       *   down (BOOLEAN, no default value) path-to-gc-roots : [optional] Collect path to GC roots (BOOLEAN, false)
       */
      def start(name: String, settings: String = null, otherArgs: String = ""): Seq[String] = {
        val cmd = s"JFR.start name=$name ${notNull(settings, "settings")} $otherArgs"
        log.info(s"CMD = $cmd")
        execute(cmd)
      }

      /**
       * Stops a JFR recording. JCMD help -
       * --- JFR.stop Stops a JFR recording
       *
       * Impact: Low
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : JFR.stop [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax) name : Recording text,.e.g
       * "MyRecording" (STRING, no default value) filename : [optional] Copy recording data to file, e.g.
       * "C:\\Users\\user\\MyRecording.jfr" (STRING, no default value)
       *
       * @param name
       *   : Recording text,.e.g "MyRecording" (STRING, no default value)
       * @param filename
       *   : [optional] Copy recording data to file, e.g. "C:\\Users\\user\\MyRecording.jfr" (STRING, no default value)
       */
      def stop(name: String, filename: String = ""): Seq[String] = {
        execute(s"JFR.stop name=$name")
      }

    }
    object VM {

      /**
       * the command line used to start this VM instance. JCMD help -
       * --- VM.command_line Print the command line used to start this VM instance.
       *
       * Impact: Low
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax: VM.command_line
       */
      def command_line: Seq[String] = {
        execute("VM.command_line")
      }

      /**
       * VM flag options and their current value. JCMD help -
       * --- Print VM flag options and their current values.
       *
       * Impact: Low
       *
       * Permission: java.lang.management.ManagementPermission(monitor)
       *
       * Syntax : VM.flags [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -all : [optional] Print all flags supported by the VM (BOOLEAN, false)
       * @param all
       *   Print all flags supported by the VM. default false
       */
      def flags(all: Boolean = false): Seq[String] = {
        execute(s"VM.flags ${bool(all, "-all")}")
      }

      /**
       * Read or change the logging. e.g. log("output=app.trace what=all=trace") // log everything to app.trace JCMD
       * help -
       * --- VM.log Lists current log configuration, enables/disables/configures a log output, or rotates all logs.
       *
       * Impact: Low: No impact
       *
       * Permission: java.lang.management.ManagementPermission(control)
       *
       * Syntax : VM.log [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax) output : [optional] The name or
       * index (#<index>) of output to configure. (STRING, no default value) output_options : [optional] Options for the
       * output. (STRING, no default value) what : [optional] Configures what tags to log. (STRING, no default value)
       * decorators : [optional] Configures which decorators to use. Use 'none' or an empty value to remove all.
       * (STRING, no default value) disable : [optional] Turns off all logging and clears the log configuration.
       * (BOOLEAN, no default value) list : [optional] Lists current log configuration. (BOOLEAN, no default value)
       * rotate : [optional] Rotates all logs. (BOOLEAN, no default value)
       */
      def log(args: String): Seq[String] = {
        execute(s"VM.log $args")
      }

      /**
       * Print system properties. JCMD help -
       * --- VM.system_properties Print system properties.
       *
       * Impact: Low
       *
       * Permission: java.util.PropertyPermission(*, read)
       *
       * Syntax: VM.system_properties
       * @see
       *   VirtualMachineUtils#getSystemProperties
       */
      def system_properties(): Seq[String] = {
        execute("VM.system_properties")
      }

      /**
       * the command line used to start this VM instance. JCMD help -
       * --- VM.uptime Print VM uptime.
       *
       * Impact: Low
       *
       * Syntax : VM.uptime [options]
       *
       * Options: (options must be specified using the <key> or <key>=<value> syntax)
       * -date : [optional] Add a prefix with current date (BOOLEAN, false)
       */
      def uptime(date: Boolean = false): Seq[String] = {
        execute(s"VM.uptime ${bool(date, "-date")}")
      }

      /**
       * JVM version information. JCMD help -
       * --- VM.version Print JVM version information.
       *
       * Impact: Low
       *
       * Permission: java.util.PropertyPermission(java.vm.version, read)
       *
       * Syntax: VM.version
       */
      def version: Seq[String] = {
        execute("VM.version")
      }
    }
  }
}
