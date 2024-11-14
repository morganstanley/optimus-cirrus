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
package optimus.examples.platform

import optimus.platform._
import optimus.platform.util.Log

/**
 * Below is the output from the program a,b,e,f are nodes c,d,g,h are non-nodes h throws an Exception a calls b calls c
 * etc
 *
 * Node trace is [start] => a => b => e => f You only see the @node that are involved. Stack trace shows f => g => h at
 * top. This is f (async) => g (sync) => h (sync) Node f is waiting for the synchronous call chain to complete.
 *
 * The stack trace for b => c => d => e is often not visible - this is because it can have been executed on another
 * thread by the Optimus scheduler. In this case node b is waiting for the synchronous call chain c (sync) => d (sync)
 * \=> e (async - this is a sync stack) to complete. This involves waiting for the result of node e. This potentially
 * would occupy the thread - waiting for the result. The Optimus runtime is clever in that it reuses the thread to
 * evaluate another node in the meantime. Unfortunately this means the full stack trace you see is not necessarily all
 * "logically" related. For this reason it is safest to ignore any part of the stack trace below the first
 * PropertyNode.lookupAndGet or Node.get
 *
 * java.lang.IllegalStateException at optimus.examples.platform.StackTraceNodeTraceFoo.h(ExceptionHandlingExample.scala:21)
 * at optimus.examples.platform.StackTraceNodeTraceFoo.g(ExceptionHandlingExample.scala:20) at
 * optimus.examples.platform.StackTraceNodeTraceFoo$$f$node$1.func(ExceptionHandlingExample.scala:19) at
 * optimus.examples.platform.StackTraceNodeTraceFoo$$f$node$1.func(ExceptionHandlingExample.scala:19) at
 * optimus.graph.PropertyNodeSync.run(PropertyNode.scala:210) at
 * optimus.graph.OGSchedulerContext.run(OGSchedulerContext.java:474) at
 * optimus.graph.OGSchedulerContext.drainQueue(OGSchedulerContext.java:335) at
 * optimus.graph.OGSchedulerContext.runAndWait(OGSchedulerContext.java:370) at
 * optimus.graph.PropertyNode.lookupAndGet(PropertyNode.scala:131) at
 * optimus.examples.platform.StackTraceNodeTraceFoo.e(ExceptionHandlingExample.scala:15) at
 * optimus.examples.platform.StackTraceNodeTraceFoo.d(ExceptionHandlingExample.scala:13) at
 * optimus.examples.platform.StackTraceNodeTraceFoo.c(ExceptionHandlingExample.scala:10) at
 * optimus.examples.platform.StackTraceNodeTraceFoo$$b$node$1.func(ExceptionHandlingExample.scala:9) at
 * optimus.examples.platform.StackTraceNodeTraceFoo$$b$node$1.func(ExceptionHandlingExample.scala:9) at
 * optimus.graph.PropertyNodeSync.run(PropertyNode.scala:210) at
 * optimus.graph.OGSchedulerContext.run(OGSchedulerContext.java:474) at
 * optimus.graph.OGSchedulerContext.drainQueue(OGSchedulerContext.java:335) at
 * optimus.graph.OGSchedulerContext.runAndWait(OGSchedulerContext.java:370) at
 * optimus.graph.PropertyNode.lookupAndGet(PropertyNode.scala:131) at
 * optimus.examples.platform.StackTraceNodeTraceFoo.a(ExceptionHandlingExample.scala:8) at
 * optimus.examples.platform.ExceptionHandlingExample$.delayedEndpoint$optimus$examples$platform$ExceptionHandlingExample$1(ExceptionHandlingExample.scala:28)
 * at optimus.examples.platform.ExceptionHandlingExample$delayedInit$body.apply(ExceptionHandlingExample.scala:24) at
 * scala.Function0$class.apply$mcV$sp(Function0.scala:40) at
 * scala.runtime.AbstractFunction0.apply$mcV$sp(AbstractFunction0.scala:12) at
 * optimus.platform.OptimusAppTrait$$anonfun$run$1.apply(LegacyOptimusApp.scala:203) at
 * optimus.platform.OptimusAppTrait$$anonfun$run$1.apply(LegacyOptimusApp.scala:203) at
 * scala.collection.immutable.List.foreach(List.scala:381) at
 * scala.collection.generic.TraversableForwarder$class.foreach(TraversableForwarder.scala:35) at
 * scala.collection.mutable.ListBuffer.foreach(ListBuffer.scala:45) at
 * optimus.platform.OptimusAppTrait$class.run(LegacyOptimusApp.scala:203) at
 * optimus.platform.LegacyOptimusApp.run(LegacyOptimusApp.scala:241) at
 * optimus.platform.OptimusAppTrait$$anonfun$main$1.apply$mcV$sp(LegacyOptimusApp.scala:179) at
 * optimus.platform.OptimusAppTrait$$anonfun$main$1.apply(LegacyOptimusApp.scala:176) at
 * optimus.platform.OptimusAppTrait$$anonfun$main$1.apply(LegacyOptimusApp.scala:176) at
 * optimus.platform.ShutdownLifeCycle$class.doUsing(RuntimeEnvironment.scala:100) at
 * optimus.platform.RuntimeEnvironment.doUsing(RuntimeEnvironment.scala:22) at
 * optimus.platform.OptimusTask$class.withOptimus(OptimusTask.scala:95) at
 * optimus.platform.LegacyOptimusApp.withOptimus(LegacyOptimusApp.scala:241) at
 * optimus.platform.OptimusAppTrait$class.main(LegacyOptimusApp.scala:176) at
 * optimus.platform.LegacyOptimusApp.main(LegacyOptimusApp.scala:241) at
 * optimus.platform.OptimusAppTrait$class.main(LegacyOptimusApp.scala:126) at
 * optimus.platform.LegacyOptimusApp.main(LegacyOptimusApp.scala:241) at
 * optimus.examples.platform.ExceptionHandlingExample.main(ExceptionHandlingExample.scala) NodeTrace 0.
 * StackTraceNodeTraceFoo.f - (ExceptionHandlingExample.scala:19) SRi:$:2~T1 1
 * \1. StackTraceNodeTraceFoo.e - (ExceptionHandlingExample.scala:15) SRi:$:2~T1 1 2. StackTraceNodeTraceFoo.b -
 * (ExceptionHandlingExample.scala:9) SRi:$:2~T1 1 3. StackTraceNodeTraceFoo.a - (ExceptionHandlingExample.scala:8)
 * SRi:$:2~T1 1 4. [start] - (RuntimeEnvironment.scala:35) SRi:$:2~T1
 */

@entity
class StackTraceNodeTraceFoo(id: Int) {
  @node def a(n: Int): Boolean = b(n)
  @node def b(n: Int): Boolean = c(n)
  def c(n: Int): Boolean = d(n)
  def d(n: Int): Boolean = {
    // to highlight effects of body vs signature on line number
    e(n)
  }
  @node def e(n: Int): Boolean = {
    // to highlight effects of body vs signature on line number
    f(n)
  }
  @node def f(n: Int): Boolean = g(n)
  def g(n: Int): Boolean = h(n)
  def h(n: Int): Boolean = throw new IllegalStateException()
}

object ExceptionHandlingExample extends LegacyOptimusApp[OptimusAppCmdLine] with Log {
  val foo = StackTraceNodeTraceFoo(1)
  try {
    foo.a(1)
  } catch {
    case e: IllegalStateException =>
      e.printStackTrace(System.out)
      System.out.println(s"NodeTrace\n${e.getNodeTrace}")
  }
}
