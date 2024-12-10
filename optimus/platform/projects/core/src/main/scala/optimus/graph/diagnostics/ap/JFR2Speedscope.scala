package optimus.graph.diagnostics.ap

/*
 * Based on Flamegraph.java and jfr2flame.java which are
 * Copyright 2020 Andrei Pangin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* This file is based on Flamegraph.java and jfr2flame.java (now JfrToFlame.java) from
 * https://github.com/async-profiler/async-profiler. Changes were made that include writing into Scala and compatibility
 * with the Speedscope Flamegraph visualizer. See comments for details about what is copied from async-profiler (a-p).
 *
 * For those pieces only:
 *
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

import java.nio.charset.StandardCharsets
import one.jfr.Dictionary
import one.jfr.JfrReader
// import one.jfr.event.CustomSample
import one.jfr.event.Event
import optimus.platform.util.Log
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import scala.collection.mutable

class JFR2SpeedscopeCmdLine {
  import org.kohsuke.args4j.{Option => ArgOption, Argument => Arg}
  @Arg(
    index = 0,
    metaVar = "INP",
    required = true,
    usage = "Path to input file in jfr format"
  )
  val inputFile: String = null

  @Arg(
    index = 1,
    metaVar = "OUT",
    required = false,
    usage = "Path to output file in speedscope format"
  )
  private val _outputFile: String = null
  val outputFile: String = Option(_outputFile).getOrElse(inputFile.stripSuffix(".jfr") + ".json")

  @ArgOption(
    name = "--start",
    required = false,
    usage = "Start at this time, in millis from start of recording or epoch"
  )
  val start = 0L

  @ArgOption(
    name = "--end",
    required = false,
    usage = "End at this time, in millis from start of recording or epoch"
  )
  val end = 0L

  @ArgOption(
    name = "--threads",
    required = false,
    usage = "Aggregate separately by thread"
  )
  val threads = false

  @ArgOption(
    name = "--noFQCN",
    required = false,
    usage = "Simple java classname"
  )
  val simple = false

  @ArgOption(
    name = "--help",
    required = false,
    aliases = Array("-h"),
    help = true,
    usage = "Print usage for this command"
  )
  val help = false
}

object JFR2Speedscope extends App with Log {
  val cmdLine = new JFR2SpeedscopeCmdLine
  val parser = new CmdLineParser(cmdLine)
  def printHelp() = {
    System.err.println("Usage: JFR2Speedscope INP OUT")
    System.err.println()
    System.err.println("Convert a JFR file from async-profiler to the speedscope json format.")
    System.err.println()
    parser.printUsage(System.err)
  }
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      System.err.println(e.getMessage)
      System.err.println("Try 'JFR2Speedscope --help' for more information.")
      System.exit(1)
  }
  if (cmdLine.help) {
    printHelp()
    System.exit(0)
  }

  import one.jfr.event.AllocationSample
  import one.jfr.event.ContendedLock
  import one.jfr.event.EventAggregator
  import one.jfr.event.ExecutionSample
  import one.jfr.event.LiveObject
  import java.util
  import scala.jdk.CollectionConverters._

  private val methodIdToMethodName = new Dictionary[String]
  // Using JFR reader and event aggregator  provided with async-profiler
  val jfr = new JfrReader(cmdLine.inputFile)
  val agg = new EventAggregator(cmdLine.threads, true)

  val speedscope = new Speedscope(cleanLambdas = true)

  // TODO (OPTIMUS-62633): This should work with a-p 3.0
  val threadState: Int = 0 // getMapKey(jfr.threadStates, "STATE_RUNNABLE")

  private def toTicks(millis: Long) = {
    var nanos = millis * 1000000
    if (millis < 0) nanos += jfr.endNanos
    else if (millis < 1500000000000L) nanos += jfr.startNanos
    jfr.nanosToTicks(nanos)
  }

  private def getMapKey(map: util.Map[Integer, String], value: String): Int = {
    for (entry <- map.entrySet.asScala) { if (value == entry.getValue) { return entry.getKey } }
    -1
  }

  val startTicks = if (cmdLine.start > 0) toTicks(cmdLine.start) else Long.MinValue
  val endTicks = if (cmdLine.end > 0) toTicks(cmdLine.end) else Long.MaxValue

  private val FRAME_SUFFIX = Array("_[0]", "_[j]", "_[i]", "", "", "_[k]", "_[1]", "_[a]", "_[a]")

  // This largely follows oss async-profiler's logic, except that we're supporting collection of multiple
  // event types.
  var event: Event = null
  // null argument means read any event
  while ({ event = jfr.readEvent(null); event } ne null)
    if (
      event.time >= startTicks && event.time <= endTicks &&
      // only impose threadState for execution samples
      (threadState < 0 || !event
        .isInstanceOf[ExecutionSample] || event.asInstanceOf[ExecutionSample].threadState == threadState)
    )
      agg.collect(event)

  agg.forEach(new EventAggregator.Visitor() {
    def visit(event: Event, value: Long): Unit = {
      val stackTrace = jfr.stackTraces.get(event.stackTraceId)
      if (stackTrace != null) {
        val methods = stackTrace.methods
        val types = stackTrace.types
        val classFrame = getClassFrame(event)
        val trace =
          new Array[Int](methods.length + (if (cmdLine.threads) 1 else 0) + (if (classFrame ne null) 1 else 0))
        if (cmdLine.threads) trace(0) = speedscope.methodIndex(getThreadFrame(event.tid))
        var idx = trace.length
        if (classFrame != null) trace({ idx -= 1; idx }) = speedscope.methodIndex(classFrame)
        for (i <- 0 until methods.length) {
          val methodName = getMethodName(methods(i), types(i))
          trace({ idx -= 1; idx }) = speedscope.methodIndex(methodName + FRAME_SUFFIX(types(i)))
        }
        val eventName = event match {
          // case c: CustomSample => c.info
          case _               => event.getClass.getSimpleName
        }
        speedscope.addTrace(eventName, value, trace)
      }
    }
  })

  speedscope.write(cmdLine.outputFile)

  private def getThreadFrame(tid: Int): String = {
    val threadName: String = jfr.threads.get(tid)
    if (threadName == null) "[tid=" + tid + ']'
    else '[' + threadName + " tid=" + tid + ']'
  }

  // These suffixes match those that async-profiler uses when generating its own html, to make frame color choices.
  // Not strictly necessary here, but keeping them around anyway.
  private def getClassFrame(event: Event): String = {
    var classId: Long = 0L
    var suffix: String = null
    if (event.isInstanceOf[AllocationSample]) {
      classId = event.asInstanceOf[AllocationSample].classId
      suffix =
        if (event.asInstanceOf[AllocationSample].tlabSize == 0) "_[k]"
        else "_[i]"
    } else if (event.isInstanceOf[ContendedLock]) {
      classId = event.asInstanceOf[ContendedLock].classId
      suffix = "_[i]"
    } else if (event.isInstanceOf[LiveObject]) {
      classId = event.asInstanceOf[LiveObject].classId
      suffix = "_[i]"
    } else return null
    val cls = jfr.classes.get(classId)
    if (cls == null) return "null"
    val className: Array[Byte] = jfr.symbols.get(cls.name)
    var arrayDepth: Int = 0
    while ({ className(arrayDepth) == '[' }) arrayDepth += 1
    val sb: mutable.StringBuilder = new mutable.StringBuilder(toJavaClassName(className, arrayDepth, true))
    while ({ { arrayDepth -= 1; arrayDepth + 1 } > 0 }) sb.append("[]")
    sb.append(suffix).toString
  }

  private def getMethodName(methodId: Long, methodType: Byte): String = {
    var result: String = methodIdToMethodName.get(methodId)
    if (result != null) { return result }
    val method = jfr.methods.get(methodId)
    if (method == null) { result = "unknown" }
    else {
      val cls = jfr.classes.get(method.cls)
      val className: Array[Byte] = jfr.symbols.get(cls.name)
      val methodName: Array[Byte] = jfr.symbols.get(method.name)
      if (className == null || className.length == 0 || isNativeFrame(methodType)) {
        result = new String(methodName, StandardCharsets.UTF_8)
      } else {
        val classStr: String = toJavaClassName(className, 0, true)
        val methodStr: String = new String(methodName, StandardCharsets.UTF_8)
        result = classStr + '.' + methodStr
      }
    }
    methodIdToMethodName.put(methodId, result)
    result
  }

  // copied from top-level async profiler FlameGraph class.  Keeping locally unused elements for clarity
  object FlameGraph {
    val FRAME_INTERPRETED = 0
    val FRAME_JIT_COMPILED = 1
    val FRAME_INLINED = 2
    val FRAME_NATIVE = 3
    val FRAME_CPP = 4
    val FRAME_KERNEL = 5
    val FRAME_C1_COMPILED = 6
    val FRAME_AWAIT_S = 7
    val FRAME_AWAIT_J = 8
  }

  // TODO (OPTIMUS-62633): This should work with a-p 3.0
  private def isNativeFrame(methodType: Byte): Boolean = false
  // methodType >= FlameGraph.FRAME_NATIVE && methodType <= FlameGraph.FRAME_KERNEL && jfr.frameTypes.size > FlameGraph.FRAME_NATIVE + 1

  private def toJavaClassName(symbol: Array[Byte], start0: Int, dotted: Boolean): String = {
    var end: Int = symbol.length
    var start = start0
    if (start > 0) {
      symbol(start) match {
        case 'B' =>
          return "byte"
        case 'C' =>
          return "char"
        case 'S' =>
          return "short"
        case 'I' =>
          return "int"
        case 'J' =>
          return "long"
        case 'Z' =>
          return "boolean"
        case 'F' =>
          return "float"
        case 'D' =>
          return "double"
        case 'L' =>
          start += 1
          end -= 1
      }
    }
    if (cmdLine.simple) {
      var i = end
      while (i >= start) {
        if (symbol(i) == '/' && (symbol(i + 1) < '0' || symbol(i + 1) > '9'))
          start = i + 1
        i -= 1
      }
    }

    val s = new String(symbol, start, end - start, StandardCharsets.UTF_8);
    if (dotted) s.replace('/', '.') else s;
  }

}
