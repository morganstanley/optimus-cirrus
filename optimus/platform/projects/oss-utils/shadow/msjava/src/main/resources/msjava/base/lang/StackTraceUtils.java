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

package msjava.base.lang;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;
public abstract class StackTraceUtils {
    private final static Logger log = LoggerFactory.getLogger(StackTraceUtils.class);
    static final String STACK_TRACE_INDENT = "\tat ";
    private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[] {};
    
    public static StackTraceElement[] getCurrentStackTrace() {
        return getCurrentStackTrace(2);
    }
    
    private static StackTraceElement[] getCurrentStackTrace(int numTraceLinesToOmmit) {
        StackTraceElement[] exceptionStackTrace = Thread.currentThread().getStackTrace();
        if (exceptionStackTrace.length <= numTraceLinesToOmmit) {
            return EMPTY_STACK_TRACE;
        }
        int i = 0;
        String threadClassName = Thread.class.getName();
        while (exceptionStackTrace.length > i && threadClassName.equals(exceptionStackTrace[i].getClassName())) {
            ++i;
        }
        numTraceLinesToOmmit += i;
        StackTraceElement[] currentStackTrace = new StackTraceElement[exceptionStackTrace.length
                - numTraceLinesToOmmit];
        System.arraycopy(exceptionStackTrace, numTraceLinesToOmmit, currentStackTrace, 0, currentStackTrace.length);
        return currentStackTrace;
    }
    
    public static String currentStackTraceToString() {
        return stackTraceToString(getCurrentStackTrace(2));
    }
    
    public static StringBuffer appendCurrentStackTrace(StringBuffer out) {
        return appendStackTrace(out, getCurrentStackTrace(2));
    }
    
    public static StringBuilder appendCurrentStackTrace(StringBuilder out) {
        return appendStackTrace(out, getCurrentStackTrace(2));
    }
    public static void logAllStackTraces(Logger log) {
        log.info(appendAllStackTraces(new StringBuilder()).toString());
    }
    
    public static <T extends Appendable> T appendAllStackTraces(T out) {
        try {
            Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
            out.append("Logging all stacktraces...\n");
            for (Map.Entry<Thread, StackTraceElement[]> threadEntry : stackTraces.entrySet()) {
                Thread thread = threadEntry.getKey();
                StackTraceElement[] stackTrace = threadEntry.getValue();
                out.append("Logging stacktrace for thread ").append(thread.toString()).append("\n");
                appendStackTrace(out, stackTrace);
                out.append("\n");
            }
        } catch (IOException e) {
            log.error("error printing stacktrace", e);
        }
        return out;
    }
    
    public static <T extends Appendable> T appendCurrentStackTrace(T out) {
        return appendStackTrace(out, getCurrentStackTrace(2));
    }
    
    public static void printCurrentStackTrace(PrintWriter out) {
        printStackTrace(out, getCurrentStackTrace(2));
    }
    
    public static void printCurrentStackTrace(PrintStream out) {
        printStackTrace(out, getCurrentStackTrace(2));
    }
    
    public static String stackTraceToString(StackTraceElement[] stackTrace) {
        return appendStackTrace(new StringBuilder(stackTrace.length * 150), stackTrace).toString();
    }
    
    public static StringBuffer appendStackTrace(StringBuffer out, StackTraceElement[] stackTrace) {
        return (StringBuffer) appendStackTrace((Appendable) out, stackTrace);
    }
    
    public static StringBuilder appendStackTrace(StringBuilder out, StackTraceElement[] stackTrace) {
        return (StringBuilder) appendStackTrace((Appendable) out, stackTrace);
    }
    
    public static <T extends Appendable> T appendStackTrace(T out, StackTraceElement[] stackTrace) {
        try {
            for (StackTraceElement frame : stackTrace) {
                out.append(STACK_TRACE_INDENT).append(String.valueOf(frame)).append("\n");
            }
        } catch (IOException e) {
            log.error("error printing stacktrace", e);
        }
        return out;
    }
    
    public static void printStackTrace(PrintWriter out, StackTraceElement[] stackTrace) {
        appendStackTrace(out, stackTrace);
    }
    
    public static void printStackTrace(PrintStream out, StackTraceElement[] stackTrace) {
        appendStackTrace(out, stackTrace);
    }
}