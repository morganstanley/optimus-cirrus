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
package optimus.graph.diagnostics.ap

import optimus.platform.util.Log
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import java.io.BufferedReader
import java.io.FileReader
import com.opencsv.CSVReader

class Collapsed2SpeedscopeCmdLine {
  import org.kohsuke.args4j.{Option => ArgOption, Argument => Arg}
  @Arg(
    index = 0,
    metaVar = "INP",
    required = true,
    usage = "Path to input file in collapsed stack format."
  )
  val inputFile: String = "collapsed.flame"

  @Arg(
    index = 1,
    metaVar = "OUT",
    required = true,
    usage = "Path to output file in speedscope format"
  )
  val outputFile: String = "speedscope.json"

  @ArgOption(
    name = "--splunk",
    required = false,
    usage = "Parse CSV from splunk: type, samples, collapsed"
  )
  val splunk = false;

  @ArgOption(
    name = "--csv1",
    required = false,
    usage = "Assume superfluous header, and quote marks"
  )
  val csv1 = false

  @ArgOption(
    name = "--maxstacks",
    required = false,
    usage = "Stop after this many stacks"
  )
  val maxStacks = Int.MaxValue

  @ArgOption(
    name = "--help",
    required = false,
    aliases = Array("-h"),
    help = true,
    usage = "Print usage for this command"
  )
  val help = false
}

/**
 * Convert Brendan Gregg style collapsed stacks into the native json format of speedscope. Since the latter is heavily
 * normalized, it will be a lot smaller. See https://github.com/BrendanGregg/flamegraph#2-fold-stacks
 * https://github.com/jlfwong/speedscope/wiki/Importing-from-custom-sources for documentation.
 */
object Collapsed2Speedscope extends App with Log {
  val cmdLine = new Collapsed2SpeedscopeCmdLine
  val parser = new CmdLineParser(cmdLine)
  def printHelp() = {
    System.err.println("Usage: Collapsed2Speedscope INP OUT")
    System.err.println()
    System.err.println("Convert a \"collapsed\" formatted file from async-profiler to the speedscope json format.")
    System.err.println()
    parser.printUsage(System.err)
  }
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      System.err.println(e.getMessage)
      System.err.println("Try 'Collapsed2Speedscope --help' for more information.")
      System.exit(1)
  }
  if (cmdLine.help) {
    printHelp()
    System.exit(0)
  }

  import cmdLine._

  val speedscope = new Speedscope(cleanLambdas = true)

  var n = 0

  if (splunk) {
    val input = new CSVReader(new FileReader(inputFile))
    input.readNext()
    val it = input.iterator()
    while (it.hasNext && n < cmdLine.maxStacks) {
      n += 1
      val Array(stackType, weightString, stack) = it.next()
      val weight = weightString.toLong
      val trace = stack.split(';').filter(_.nonEmpty).map(speedscope.methodIndex)
      speedscope.addTrace(stackType, weight, trace)
    }
    input.close()
  } else {
    val input = new BufferedReader(new FileReader(inputFile))
    log.info(s"Reading $inputFile")
    if (csv1) {
      log.info(s"Stripping CSV cruft")
      input.readLine()
    }
    var line: String = input.readLine()
    while (line != null && n < maxStacks) {
      if (csv1)
        line = line.replaceAllLiterally("\"", "")
      n += 1
      val (stack, weight) = StackAnalysis.splitStackLine(line)
      val trace = stack.split(';').filter(_.nonEmpty).map(speedscope.methodIndex)
      speedscope.addTrace("exec", weight, trace)
      line = input.readLine()
    }
    input.close()
  }

  speedscope.write(outputFile)

}
