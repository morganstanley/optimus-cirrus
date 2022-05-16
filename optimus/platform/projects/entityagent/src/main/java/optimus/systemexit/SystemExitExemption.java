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
package optimus.systemexit;

/**
 * Exempt certain classes from having their System.exit calls rewritten.
 * These classes need to take down the whole JVM, even in tests, because the scheduler
 * is left in an inconsistent state, and often the {@link SystemExitInterceptedException}
 * is ignored or thrown on a thread where it does not show up as a test failure.
 * Such failures do, however, manifest as different and subtle bugs (broken nodes,
 * false stalls not attributed to the test in question, ...).
 */
final class SystemExitExemption {
  private static final String[] EXEMPTIONS =
  { "optimus/graph/NodeTask" // logAndDie on various unrecoverable illegal states
  , "optimus/graph/GCNative" // die when not loaded properly and when we fails completely to free up any memory
  , "optimus/graph/OGScheduler" // like NodeTask, die when we have no hope of remaining consistent
  , "optimus/graph/OGSchedulerContext" // die on exception thrown from adapt()
  , "optimus/graph/OGTraceStore" // die if the trace gets knackered somehow
  , "optimus/graph/DefaultStallDetector" // die on stall (when requested)
  };

  static boolean isClassExempt(String cls) {
    if (cls != null)
      for (String exemption : EXEMPTIONS) {
        if (cls.equals(exemption))
          return true;
      }

    return false;
  }
}
