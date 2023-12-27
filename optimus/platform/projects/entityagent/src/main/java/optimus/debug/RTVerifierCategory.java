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
package optimus.debug;

/**
 * Category that could be searched in splunk Sample: source=RT
 * payload.rtvViolation=MODULE_CTOR_EC_CURRENT | stats count by payload.rtvLocation
 *
 * <p>These names must match those used in gridprofiler_rtv_categories.js
 */
public class RTVerifierCategory {
  public static final String NONE = "RTV_CATEGORY_UNKNOWN";
  public static final String MODULE_CTOR_EC_CURRENT = "MODULE_CTOR_EC_CURRENT";

  // currently hidden in reports and dashboards, as not an RT violation until we introduce
  // multi-tenancy
  public static final String MODULE_CTOR_SI_NODE = "MODULE_CTOR_SI_NODE";
  public static final String MODULE_LAZY_VAL_EC_CURRENT = "MODULE_LAZY_VAL_EC_CURRENT";
  public static final String TWEAK_IN_ENTITY_CTOR = "TWEAK_IN_ENTITY_CTOR";
  public static final String TWEAKABLE_IN_ENTITY_CTOR = "TWEAKABLE_IN_ENTITY_CTOR";
  public static final String NON_RT_NODE = "NON_RT_NODE";
  public static final String NODE_WITH_NON_RT_CALL = "NODE_WITH_NON_RT_CALL";
  public static final String UTRACK_OVER_INVALIDATION = "UTRACK_OVER_INVALIDATION";
  public static final String UI_INVALIDATION_AFTER_EVAL = "UI_INVALIDATION_AFTER_EVAL";
}
