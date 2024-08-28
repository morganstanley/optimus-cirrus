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
package optimus.platform.stats

import java.util.concurrent.ConcurrentHashMap

/**
 * This is a runtime utility to interpret an AppStatsControl/Run against the current application and maintain a pool of
 * appropriate AppStats to use.
 *
 * The AppStatsControl specifies some constant information about this "run" of the application. In particular, a list of
 * "components" which indicate which profiling should be performed.
 *
 * This class will accept declarations of the "current component" from the application, interpret the AppStatsControl
 * requests, and maintain a pool of either live AppStats or disabled instances. Thus the application can reference this
 * pool to get the correct AppStats, and issue the correct probeStart...probeEnd or profileWith{...} calls.
 *
 * If profiling overall, or for this particular location, is disabled then a dummy disabled instance will be returned.
 * The application does not need to be aware of this - it just performs the normal probe/profile calls.
 */
object AppStatsPool {

  private var lstPool: List[AppStatsPool] = Nil

  /** For the given AppStatsRun, return the AppStats to use for this Component */
  def get(forRun: AppStatsRun, componentIn: String, useThis: AppStats = null): AppStats =
    getPool(forRun).get(componentIn, useThis)

  /** Get the unique AppStatsPool for this Run */
  private def getPool(forRun: AppStatsRun): AppStatsPool = {
    var p: AppStatsPool = null

    lstPool.exists(pl =>
      if (pl.run == pl.run) { p = pl; true }
      else false)

    if (p == null) {
      p = new AppStatsPool(forRun)
      lstPool ::= p
    }
    p
  }

  /**
   * Control construct to profile a section of code. On any Exception, will unRegister the AppStats and re-throw the
   * Exception. WARNING: This serializes the execution of 'fn'
   */
  def profileWith[T](
      pool: AppStatsPool,
      components: Iterable[String],
      onComplete: AppStats => Unit = AppStatsControl.onProfileDoneEmpty,
      finish: Boolean = true,
      unRegister: Boolean = false,
      lastProbe: Boolean = false,
      onExceptionUnRegister: Boolean = false)(fn: => T): T = {

    val profilers = pool.getAll(components)
    try {
      profilers.foreach(_.probeStart(lastProbe))

      val rslt = fn // execute the user logic

      profilers.foreach(p => {
        p.probeEnd(lastProbe)
        if (finish) p.markAsFinished()
        onComplete(p)
        if (unRegister) p.unRegister()
      })
      rslt
    } catch {
      case ex: Throwable => {
        profilers.map(app => {
          app.recordException(ex)
          if (onExceptionUnRegister) app.unRegister()
        })
        throw ex
      }
    }
  }
}

/** The (unique) pool for a given AppStatsRun */
class AppStatsPool(val run: AppStatsRun) {
  val u = Utils
  val cntl = run.control
  // the pool of Component definitions which have already been processed and
  // associated with the correct AppStats instance
  private val pool = new ConcurrentHashMap[String, AppStats]()

  /** Create a specific AppStats to associate with the * / * component entry */
  def createForAll(
      flavor: AppStatsType.Value = AppStatsType.Standard,
      plusProperties: Map[String, String] = null,
      exceptionCapture: Int = run.exceptionCapture,
      register: Boolean = run.register,
      deep: Boolean = run.deep,
      suppressEmpty: Boolean = run.suppressEmpty,
      disabled: Boolean = run.disabled): AppStats =
    forAll(
      run.createAppStats(
        flavor,
        AppStatsControl.sMatchAll,
        plusProperties,
        exceptionCapture,
        register,
        deep,
        suppressEmpty,
        disabled))

  /** Set a specific AppStats to associate with the * / * entry */
  def forAll(appStats: AppStats): AppStats = pool.put(AppStatsControl.sMatchAll, appStats)

  /**
   * For this component, return the AppStats which should be used.
   *
   * The component should be of the form: name1/name2/..../nameN
   *
   * this will be matched against the components for which statistics are specified in the AppStatsControl, and the
   * appropriate AppStats returned.
   *
   * @param useThis
   *   \- may provide an AppStats with desired traits to be used. Will be saved in the pool, so future requests for the
   *   same component will return the same instance. Allows a caller to pre-populate the pool if desired.
   * @return
   *   AppStats instance - may be the 'disabled' instance if stats not specified
   */
  def get(componentIn: String, useThis: AppStats = null): AppStats = {
    // SHORT CIRCUIT - if run is disabled, always return disabled instance
    //
    // To do EVERYTHING, should have */* in component list. Specific components may
    // still have specific AppStats.
    //
    if (
      run == null || !run.gblENABLE
      || cntl.components == Nil
      || componentIn == null || componentIn.isEmpty
    )
      AppStatsControl.disabledInstance
    else {
      var stats = pool.get(componentIn)
      if (stats == null) {
        if (useThis != null) {
          stats = useThis
          pool.put(componentIn, stats)
        } else {
          // Use 'exists' to exit on first match
          cntl.components.exists {
            case (str, apptype) => {
              if (u.pathMatches(str, componentIn, trimParts = true)) {
                stats = apptype match {
                  case AppStatsType.Disabled  => AppStatsControl.disabledInstance
                  case AppStatsType.Standard  => new AppStatsStandard(run, componentIn)
                  case AppStatsType.Basic | _ => new AppStatsBasic(run, componentIn)
                }
                true
              } else false
            }
          }
          // If no particular match, see if the "*/*" entry exists
          if (stats == null && cntl.containsStarStar) {
            stats = pool.get(AppStatsControl.sMatchAll)
            if (stats == null) { // Need to create an entry
              cntl.components.exists { case (str, apptype) =>
                if (str == AppStatsControl.sMatchAll) {
                  stats = createForAll(apptype)
                  true
                } else
                  false
              }
            }
          }
        }
        // If did not find anything, use disabled
        if (stats == null) stats = AppStatsControl.disabledInstance
      }
      stats
    }
  }

  /** Return a list of AppStats to use for the given list of Components */
  def getAll(components: Iterable[String]): List[AppStats] = {
    var lst: List[AppStats] = Nil
    if (components != null) components.map(str => get(str) :: lst)
    lst
  }
}
