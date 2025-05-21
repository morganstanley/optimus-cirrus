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
package msjava.base.scv;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLHandshakeException;
import msjava.base.slf4j.ContextLogger;
import msjava.base.util.HostCanonicalizationUtils;
import msjava.base.util.internal.SystemPropertyUtils;
import com.ms.infra.kerberos.configuration.MSKerberosConfiguration;
import msjava.kerberos.auth.diag.DiagnosticsContext;
import msjava.kerberos.auth.diag.DiagnosticsContextBuilder;
import msjava.kerberos.auth.diag.GSSDiagnostics;
import org.slf4j.Logger;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
public final class SecureCredentialsVault {
    private static final Logger LOG = ContextLogger.safeLogger();
    private static final boolean DEFAULT_SKIP_HOST_CANONICALIZATION = SystemPropertyUtils
            .getBoolean("msjava.base.scv.skiphostcanonicalization", false, LOG);
    
    private static final int TIMEOUT = SystemPropertyUtils
        .getInteger("msjava.base.scv.timeout", 60000, LOG);
    private final String targetHost;
    private boolean skipHostCanonicalization = DEFAULT_SKIP_HOST_CANONICALIZATION;
    private int timeout = TIMEOUT;
    private final int targetPort;
    private final JsonFactory jsonFactory = new JsonFactory();
    
    public SecureCredentialsVault() throws SecureCredentialsVaultException {
        this("DEFAULT");
    }
    
    public SecureCredentialsVault(final String hostport) throws SecureCredentialsVaultException {
        this(hostport, DEFAULT_SKIP_HOST_CANONICALIZATION);
    }
    
    public SecureCredentialsVault(final String hostport, boolean skipHostCanonicalization)
            throws SecureCredentialsVaultException {
        MSKerberosConfiguration.getDefault().setClientConfiguration();
        this.skipHostCanonicalization = skipHostCanonicalization;
        String[] hp = hostport.split(":");
        targetHost = hp[0];
        if (hp.length == 2) {
            targetPort = Integer.parseInt(hp[1]);
        } else {
            targetPort = -1;
        }
    }
    
    public void setSkipHostCanonicalization(boolean skipHostCanonicalization) {
        this.skipHostCanonicalization = skipHostCanonicalization;
    }
    
    public boolean skipHostCanonicalization() {
        return skipHostCanonicalization;
    }
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    public int timeout() {
        return timeout;
    }
    
    public char[] getTextCred(final String namespace, final String keyname) throws SecureCredentialsVaultException {
        HttpsURLConnection connection = null;
        BufferedReader in = null;
        try {
            connection = createConnection(namespace, keyname, false);
            checkReturnedCodeForError(namespace, keyname, connection);
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
            JsonParser jp = jsonFactory.createParser(in);
            jp.nextToken(); 
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jp.getCurrentName();
                jp.nextToken();
                if ("data".equals(fieldname)) {
                    char[] fullText = jp.getTextCharacters();
                    int offset = jp.getTextOffset();
                    int length = jp.getTextLength();
                    char[] credential = Arrays.copyOfRange(fullText, offset, offset + length);
                    LOG.info("Credential is retrieved from SCV for namespace={} and keyname={}", namespace, keyname);
                    return credential;
                }
            }
            LOG.warn("Could not retrieve the credential for namespace={} and keyname={}", namespace, keyname);
            throw new SecureCredentialsVaultException("Could not retrieve the credential");
        } catch (SSLHandshakeException sslEx) {
            throw new SecureCredentialsVaultException(
                    "SSL handshake failed -- check the certificate for SCV is present in the TrustStore", sslEx);
        } catch (Exception e) {
            if (connection != null) {
                GSSDiagnostics.onError(
                        new DiagnosticsContextBuilder().setThrowable(e).setSpnegoHost(connection.getURL().getHost())
                                .addComponents(EnumSet.of(DiagnosticsContext.Component.SCV)).build());
            }
            if (e instanceof SecureCredentialsVaultException)
                throw (SecureCredentialsVaultException) e;
            throw new SecureCredentialsVaultException("Could not retrieve the credential", e);
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) { 
                LOG.warn("Could not close reader", e);
            }
        }
    }
    
    public byte[] getBinaryCred(final String namespace, final String keyname) throws SecureCredentialsVaultException {
        HttpsURLConnection connection = null;
        BufferedReader in = null;
        try {
            connection = createConnection(namespace, keyname, true);
            checkReturnedCodeForError(namespace, keyname, connection);
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
            JsonParser jp = jsonFactory.createParser(in);
            jp.nextToken(); 
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jp.getCurrentName();
                jp.nextToken();
                if ("data".equals(fieldname)) {
                    LOG.info("Credential is retrieved from SCV for namespace={} and keyname={}", namespace, keyname);
                    byte[] value = jp.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
                    return value;
                }
            }
            LOG.warn("Could not retrieve the credential for namespace={} and keyname={}", namespace, keyname);
            throw new SecureCredentialsVaultException("Could not retrieve the credential");
        } catch (SSLHandshakeException sslEx) {
            throw new SecureCredentialsVaultException(
                    "SSL handshake failed -- check the certificate for SCV is present in the TrustStore", sslEx);
        } catch (Exception e) {
            if (connection != null) {
                GSSDiagnostics.onError(
                        new DiagnosticsContextBuilder().setThrowable(e).setSpnegoHost(connection.getURL().getHost())
                                .addComponents(EnumSet.of(DiagnosticsContext.Component.SCV)).build());
            }
            if (e instanceof SecureCredentialsVaultException)
                throw (SecureCredentialsVaultException) e;
            throw new SecureCredentialsVaultException("Could not retrieve the credential", e);
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) { 
                LOG.warn("Could not close reader", e);
            }
        }
    }
    
    private void checkReturnedCodeForError(final String namespace, final String keyname, HttpsURLConnection connection)
            throws IOException, UnsupportedEncodingException, SecureCredentialsVaultException {
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            InputStream errorStream = connection.getErrorStream();
            String stringFromStream = errorStream == null ? ""
                    : CharStreams.toString(new InputStreamReader(errorStream, StandardCharsets.UTF_8)).trim();
            LOG.warn("Could not retrieve the credential for namespace={} and keyname={}", namespace, keyname);
            LOG.warn("Server responded with code: {}, message: [{}]", responseCode, stringFromStream);
            throw new SecureCredentialsVaultException(
                    "Could not retrieve the credential, service code was " + responseCode + ": " + stringFromStream);
        }
    }
    @VisibleForTesting
    URI getConnectionUri(final String namespace, final String keyname, final Boolean askBase64)
            throws URISyntaxException, UnknownHostException {
        String urlQuery = "namespace=" + namespace + "&keyname=" + keyname
                + (askBase64 == true ? "&_EncodeBinaryDataAs=base64" : "");
        String host = skipHostCanonicalization() ? targetHost
                : HostCanonicalizationUtils.getCanonicalHostName(targetHost);
        URI scvUri = new URI("https", null, host, targetPort, "/rest-json/TellKey", urlQuery, null);
        LOG.debug("Formed URI to connect to SCV is {} ({}canonicalized)", scvUri,
                skipHostCanonicalization() ? "not " : "");
        return scvUri;
    }
    
    private HttpsURLConnection createConnection(final String namespace, final String keyname, final Boolean askBase64)
            throws URISyntaxException, UnknownHostException, IOException, MalformedURLException {
        URI scvUri = getConnectionUri(namespace, keyname, askBase64);
        HttpsURLConnection connection = (HttpsURLConnection) scvUri.toURL().openConnection();
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
        return connection;
    }
}
