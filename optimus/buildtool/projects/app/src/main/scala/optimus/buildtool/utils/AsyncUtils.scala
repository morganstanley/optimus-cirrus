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
package optimus.buildtool.utils

import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.Seq

object AsyncUtils {

  @alwaysAutoAsyncArgs def asyncTry[A](tryF: => A): AsyncTryer[A] = needsPlugin
  @async def asyncTry[A](tryF: AsyncFunction0[A]): AsyncTryer[A] = new AsyncTryer[A](tryF)
  class AsyncTryer[A](tryF: AsyncFunction0[A], finallyFs: Seq[AsyncFunction0[Any]] = Nil) {

    @alwaysAutoAsyncArgs def thenFinally[B](finallyF: => B): AsyncTryer[A] = needsPlugin
    @async def thenFinally[B](finallyF: AsyncFunction0[B]): AsyncTryer[A] = {
      new AsyncTryer[A](tryF, finallyFs :+ finallyF)
    }

    @alwaysAutoAsyncArgs def asyncFinally[B](finallyF: => B): A = needsPlugin
    @async def asyncFinally[B](finallyF: AsyncFunction0[B]): A = thenFinally(finallyF).run()

    @async def run(): A = {
      val tryRes = asyncResult(EvaluationContext.cancelScope)(tryF())
      val exception = finallyFs.foldLeft(Option(tryRes.exception)) { (e, finallyF) =>
        val finallyRes = asyncResult(EvaluationContext.cancelScope)(finallyF())
        if (finallyRes.hasException) e match {
          case Some(e) => e.addSuppressed(finallyRes.exception); Some(e)
          case None    => Some(finallyRes.exception)
        }
        else None
      }
      exception.foreach(throw _)
      tryRes.value
    }
  }

}
