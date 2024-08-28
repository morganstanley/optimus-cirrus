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
package optimus.graph.cache;

public interface StargazerNodeHotSwapData {
  // need three data structures to support ultra fast look up, as this is on critical path of node
  // evaluation
  // 1 check if classes ClassLoader is from Stargazer
  // 1.1 if true use isLatestHotCompiledClass to check if Class is in latest set, if also true
  // return
  // 2 use hasReplacement class to look up className to see if it has a replacement Stargazer
  // compiled version
  // if false return
  // if true enter the node substitution code path
  boolean isLatestHotCompiledClass(Class<?> clazz);

  boolean hasReplacementClass(String className);

  Class<?> getClass(String className);

  StargazerNodeHotSwapData extendWith(StargazerNodeHotSwapData that);
}
