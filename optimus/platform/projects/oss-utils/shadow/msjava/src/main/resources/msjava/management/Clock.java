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

package msjava.management;
public interface Clock {
    
    enum Type {
        MILLISEC, NANOSEC
    }
    
    public long getCurrentTime();
    
    public String getUnitOfMeasurement();
    
    public static class MilliSecondClock implements Clock {
        @Override
        public long getCurrentTime() {
            return System.currentTimeMillis();
        }
        @Override
        public String getUnitOfMeasurement() {
            return "Milli second";
        }
    }
    
    public static class NanoSecondClock implements Clock {
        @Override
        public long getCurrentTime() {
            return System.nanoTime();
        }
        @Override
        public String getUnitOfMeasurement() {
            return "Nano second";
        }
    }
}
