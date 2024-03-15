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
package optimus.platform.tests.dtc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation means that the test class is not running optimally in our development
 * environment.
 *
 * <p>It is for tests using any kind of JUnit-rooted test runner (that is, anything pass/fail). It
 * indicates that the test runner shall bypass using DTC for that test class on CI and locally.
 *
 * <p>DTC will evaluate your change and determine if the test class(es) you are running are
 * genuinely affected by the change. If there is a successful run stored in the cache which is
 * guaranteed to have the same runtime, then its result is used and the test class is not executed.
 * If there is no matching run, the test is run and used to populate the cache upon success.
 *
 * <p>The main cost of DTC is that the runtime behaviour of the test is monitored, potentially
 * increasing memory usage. When this annotation is used, it likely means either: 1. The runtime
 * class-usage monitor does not work properly on the test. 2. It is a special kind of test which DTC
 * cannot handle yet. 3. The behaviour of the test is not "RT-ish" enough to give confidence in the
 * correctness of a cache hit.
 *
 * <p>For more information about DTC and how to remove this annotation, please see
 * DistributedTestCaching in the Optimus Developer Guide
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.TYPE)
public @interface SubOptimalTesting {
  /** The reason why the test class is not using DTC. */
  Reason why();

  /** Specific explanation of the reasoning. */
  String because() default "";

  enum Reason {
    /* This indicates a problem with the test: non-RT behaviour, suspicious resource usage, or timing-related assertions.
     * It is also for test classes which are dependent on the execution of another test class.
     *
     * We could cache it or use the dry-run but we do not trust the cache hits.
     * Resolution is usually the responsibility of the test's owner.
     */
    Unsafe,
    /* Indicates a test which is not currently supported in our framework, due to its configuration or execution.
     * Includes custom runners that we can't handle yet.
     *
     * We can't run the test using DTC instrumentation. It could cause exceptions, crashes, or memory/performance problems.
     * Resolution is usually the responsibility of DTC developers.
     */
    NotSupported
  }
}
