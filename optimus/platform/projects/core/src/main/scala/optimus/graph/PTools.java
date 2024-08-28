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
package optimus.graph;

import optimus.config.InstallPathLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Interface to Intel VTune and other profiler native helpers
public class PTools {
  private static final sun.misc.Unsafe U = UnsafeAccess.getUnsafe();

  public static native long rdtscp();

  public static native long baseFrequency();

  public static final boolean samplingSupported;
  /** Currently only pre-loaded agent is supported and only a single feature supported */
  private static native long samplingFeaturesSupported();

  public static native long startSampling();

  public static native long stopSampling();

  public static native long startSamplingCurrentThread();

  public static native long stopSamplingCurrentThread();

  public static native long getThreadStack(long id);

  public static native Object[] getHotSpots(long[] prefix);

  public static void registerCurrentThreadForSampling(PThreadContext ctx) {
    if (samplingSupported) ctx.jvmProfilingCookie = startSamplingCurrentThread();
  }

  public static void startSamplingThread(PThreadContext ctx) {
    if (samplingSupported && ctx.jvmProfilingCookie != 0) U.putLong(ctx.jvmProfilingCookie, 1);
  }

  public static void stopSamplingThread(PThreadContext ctx) {
    if (samplingSupported && ctx.jvmProfilingCookie != 0) U.putLong(ctx.jvmProfilingCookie, 0);
  }

  private static Logger log = LoggerFactory.getLogger(PTools.class);

  static {
    if (DiagnosticSettings.samplingProfilerStatic) samplingSupported = false;
    else {
      long featuresSupported = 0;
      try {
        featuresSupported = samplingFeaturesSupported();
      } catch (UnsatisfiedLinkError ignored) {
      } // Agent is't available
      samplingSupported = featuresSupported > 0;

      InstallPathLocator.Resolver resolver = InstallPathLocator.system();
      try {
        resolver.loadPTools();
      } catch (UnsatisfiedLinkError ex) {
        log.info("Ptools loading failed with exception {}", ex.toString());
      }
    }
  }
}
