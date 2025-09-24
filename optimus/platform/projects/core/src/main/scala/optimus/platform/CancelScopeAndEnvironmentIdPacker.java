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
package optimus.platform;

import optimus.graph.GraphException;
import optimus.graph.diagnostics.InfoDumper;
import scala.Function0;

/**
 * Packs CancellationScopeId and RuntimeEnvironmentId into 64-bit field.
 *
 * <p>Written in Java so that we can have efficient static method calls and efficient constant
 * access
 */
final class CancelScopeAndEnvironmentIdPacker {

  // allow more bits for the CancellationScope since we expect to have far more of these
  // than we have RuntimeEnvironments
  private static final int bitsForEnvId = 24;
  private static final int bitsForCsId = 64 - bitsForEnvId;

  public static final long maxEnvId = 1L << bitsForEnvId;
  public static final long maxCsId = 1L << bitsForCsId;
  private static final long csIdMask = (1L << bitsForCsId) - 1;

  public static long packCancelScopeAndEnvId(long csId, int envId) {
    checkLimit("RuntimeEnvironment", envId, maxEnvId);
    checkLimit("CancellationScope", csId, maxCsId);

    return ((long) envId << bitsForCsId) | csId;
  }

  public static long unpackCancelScopeId(long packedId) {
    return packedId & csIdMask;
  }

  public static long unpackEnvId(long packedId) {
    // n.b. no need to apply a mask since there is nothing to the left of the packed envId
    return packedId >>> bitsForCsId;
  }

  private static boolean overflowIsFatal = true;

  private static void checkLimit(String type, long actual, long limit) {
    if (actual >= limit) {
      GraphException ex =
          new GraphException(type + " Id " + actual + " exceeded max id " + (limit - 1));
      if (overflowIsFatal)
        InfoDumper.graphPanic(
            type + " Id overflow",
            actual + " exceeded max id " + (limit - 1),
            87,
            ex,
            EvaluationContext$.MODULE$.currentNodeOrNull());
      else throw ex;
    }
  }

  static <T> T withNonFatalLimits(Function0<T> func) {
    boolean old = overflowIsFatal;
    overflowIsFatal = false;
    try {
      return func.apply();
    } finally {
      overflowIsFatal = old;
    }
  }
}
