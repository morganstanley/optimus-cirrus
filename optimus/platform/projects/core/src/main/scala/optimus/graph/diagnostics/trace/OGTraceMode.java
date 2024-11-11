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
package optimus.graph.diagnostics.trace;

import optimus.graph.DiagnosticSettings;

public class OGTraceMode {
  public static final boolean testNewHS = DiagnosticSettings.getBoolProperty("testNewHS", true);
  public static final OGEventsObserver[] modes;

  public static final OGEventsObserver none = new OGEventsNullObserver();
  public static final OGEventsObserver edges = new OGEventsEdgesObserver();
  public static final OGEventsObserver environ = new OGEventsEnvObserver();
  public static final OGEventsObserver light = new OGEventsLightObserver();
  public static final OGEventsObserver timelineLight = new OGEventsTimeLineObserver();

  /**
   * modes that extend hotspotsLight, ie, not writing to events trace directly or allocating
   * PNodeTasks
   */
  public static final OGEventsObserver hotspotsLight = new OGEventsNewHotspotsObserver();

  /** modes that extend hotspots, ie, writing to events trace directly */
  public static final OGEventsObserver hotspots =
      testNewHS ? hotspotsLight : new OGEventsHotspotsObserver();

  public static final OGEventsObserver hotspotsTimeline =
      new OGEventsHotspotsWithTimelineObserver();

  public static final OGEventsObserver hashCounts = new OGEventsHotspotsWithHashesObserver();
  public static final OGEventsObserver traceNodes = new OGEventsHotspotsWithFullTraceObserver();
  public static final OGEventsObserver recording = new OGEventsRecordingObserver();
  public static final OGEventsObserver recordingWithHashes =
      new OGEventsRecordingObserverWithHashes();

  public static final OGEventsObserver recordingLight = new OGEventsRecordingLightObserver();

  private static final OGEventsObserver distributedTasks = new OGEventsDistributedTasksObserver();

  // WARNING: if testNewHS, this will return hotspotsLight even if 'hotspots' is specified
  public static OGEventsObserver find(String name) {
    for (OGEventsObserver mode : modes) {
      if (mode.name().equalsIgnoreCase(name)) {
        return mode;
      } else if (testNewHS
          && name.equalsIgnoreCase("hotspotslight")
          && mode.name().equalsIgnoreCase("hotspots")) {
        return mode; // allow hotspotslight to call itself "hotspots" when testNewHS is on. In that
        // mode both "hotspots" and "hotspotslight" select OGEventsNewHotspotsObserver
      }
    }
    return null; // Should we throw??? Default to something???
  }

  static {
    modes =
        new OGEventsObserver[] {
          none,
          edges,
          hotspotsLight,
          hotspotsTimeline,
          hashCounts,
          traceNodes,
          recording,
          recordingWithHashes,
          recordingLight,
          light,
          environ,
          timelineLight,
          distributedTasks
        };
  }
}
