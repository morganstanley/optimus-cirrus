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

public final class EvaluationState {
  /** Avoid giving warning in the places that are 'OK' to have sync stacks in */
  public static final int IGNORE_SYNC_STACKS = 1;
  /**
   * Flag is set on a scenario stack of any cacheable node and all of its children This allows for
   * quick detection whether it is safe to call non-RT method. As the result of the whole operation
   * will not be cached
   */
  public static final int CACHED_TRANSITIVELY = 2;

  /**
   * Flag is set on scenario stack that has an SI ancestor and/or is SI itself (this flag is
   * inherited by children)
   */
  public static final int SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR = 4;
  /** Flag is set on the entry SI scenario stack (this flag is NOT inherited by children) */
  public static final int SCENARIO_INDEPENDENT_STACK = 8;
  /**
   * All of the SI related flags. Note that if SCENARIO_INDEPENDENT_STACK is set then
   * SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR is always set, but the converse is not always true
   */
  public static final int ALL_SI_FLAGS =
      SCENARIO_INDEPENDENT_STACK | SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR;

  /** Set on scenario stacks that have to collect tweaks usage */
  public static final int RECORD_TWEAK_USAGE = 0x10;
  /** Set on scenario stacks that have to collect tweaks usage by modifying xinfo */
  public static final int TRACK_TWEAK_USAGE_PER_NODE = 0x20;
  /** Set on scenario stacks that have to collect tweaks usage for when-clauses */
  public static final int RECORD_WHEN_DEPENDENCIES = 0x40;
  /** At least some tweak/dal usage trackingis ON */
  public static final int TRACK_OR_RECORD_TWEAKS = RECORD_TWEAK_USAGE | TRACK_TWEAK_USAGE_PER_NODE;
  /** At least some tweak/dal usage is ON */
  public static final int NON_BASIC_STACK =
      TRACK_OR_RECORD_TWEAKS | SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR;

  /** Throw exception if scheduler encounters sync stacks (unless IGNORE_SYNC_STACKS is also set) */
  public static final int FAIL_ON_SYNC_STACKS = 0x80;
  /**
   * Contained in non-concurrent sequence, eg, fold, where results rely on previous results so
   * concurrency == 1
   */
  public static final int IN_NON_CONCURRENT_SEQ = 0x100;
  /** Flag is set if a false circular reference could be caused by waiting for an XS node */
  public static final int NO_WAIT_FOR_XS_NODE = 0x200;
  /** Flag is set to true on stateless "constant" scenario stacks where node evaluation is banned */
  public static final int CONSTANT = 0x400;

  /** Allow auditor to execute node on graph and being able to ignore those */
  public static final int AUDITOR_CALLBACKS_DISABLED = 0x800;

  /**
   * Do not cache across this scenario stack barrier, because it could cause (for example) batching
   * logic to break set for stacks that introduced NonTransparentForCaching plugin tag AND the
   * immediate child
   */
  public static final int NOT_TRANSPARENT_FOR_CACHING = 0x1000;
  /** set for stacks that introduced NonTransparentForCaching plugin tag */
  public static final int NOT_TRANSPARENT_FOR_CACHING_INHERIT = 0x2000;
  /** set for stacks that introduced NonTransparentForCaching plugin tag */
  public static final int NOT_TRANSPARENT_FOR_CACHING_BARRIER =
      NOT_TRANSPARENT_FOR_CACHING | NOT_TRANSPARENT_FOR_CACHING_INHERIT;

  /**
   * Children are never SI (you are entering a given block) and we want to clear
   * NOT_TRANSPARENT_FOR_CACHING_INHERIT
   */
  public static final int CLEAR_FOR_CHILDREN =
      SCENARIO_INDEPENDENT_STACK | NOT_TRANSPARENT_FOR_CACHING_INHERIT;

  public static int clearForChild(int state) {
    return CLEAR_FOR_CHILDREN
        | (((state & NOT_TRANSPARENT_FOR_CACHING_INHERIT) != 0) ? 0 : NOT_TRANSPARENT_FOR_CACHING);
  }

  /** Updates flags for usage by RecordingScenarioStack */
  public static int flagsForRecordingScenarioStack(int state, boolean noWaitForXS) {
    int flags = (state & ~TRACK_TWEAK_USAGE_PER_NODE) | RECORD_TWEAK_USAGE | CACHED_TRANSITIVELY;
    if (noWaitForXS) flags = flags | NO_WAIT_FOR_XS_NODE;
    return flags;
  }

  public static final int TRACKING_SCENARIO_STACK_FLAGS =
      TRACK_TWEAK_USAGE_PER_NODE | CACHED_TRANSITIVELY;

  public static final int TRACKING_SCENARIO_STACK_CLEAR_FLAGS =
      CONSTANT
          | IN_NON_CONCURRENT_SEQ
          | SCENARIO_INDEPENDENT_STACK
          | RECORD_TWEAK_USAGE
          | RECORD_WHEN_DEPENDENCIES
          | NO_WAIT_FOR_XS_NODE;

  public static final int FLAGS_TO_PROPAGATE_TO_SI_STACK =
      IGNORE_SYNC_STACKS
          | CACHED_TRANSITIVELY
          | AUDITOR_CALLBACKS_DISABLED
          | FAIL_ON_SYNC_STACKS
          | IN_NON_CONCURRENT_SEQ
          | NO_WAIT_FOR_XS_NODE;

  public static final int ENGINE_PROPAGATION_CLEAR_FLAGS =
      IGNORE_SYNC_STACKS | FAIL_ON_SYNC_STACKS | IN_NON_CONCURRENT_SEQ | NO_WAIT_FOR_XS_NODE;

  /** Updates flags for usage by DependencyTracker */
  public static int flagsForMutableScenarioStack(int state) {
    return (state | TRACKING_SCENARIO_STACK_FLAGS) & ~TRACKING_SCENARIO_STACK_CLEAR_FLAGS;
  }

  public static String toString(int flags) {
    // Tweaks of When clauses will be reported
    return (((flags & RECORD_WHEN_DEPENDENCIES) != 0) ? ".when" : "")
        + (((flags & RECORD_TWEAK_USAGE) != 0) ? ".rec" : "") // Recording
        + (((flags & TRACK_TWEAK_USAGE_PER_NODE) != 0) ? ".trk" : "") // Tracking
        + (((flags & CACHED_TRANSITIVELY) != 0) ? "" : ".-$") // No caching is not a common case
        + (((flags & IGNORE_SYNC_STACKS) != 0) ? ".-sync" : "") //
        + (((flags & SCENARIO_INDEPENDENT_STACK) != 0) ? ".si" : "") // See comments in flags
        + (((flags & (SCENARIO_INDEPENDENT_STACK | SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR))
                == SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR)
            ? ".sip"
            : "") // i.e. SCENARIO_INDEPENDENT_SELF_OR_ANCESTOR but not SCENARIO_INDEPENDENT_STACK
        + (((flags & NO_WAIT_FOR_XS_NODE) != 0) ? ".XS-nw" : "")
        + (((flags & CONSTANT) != 0) ? ".const" : "");
  }
}
