package msjava.msnet.ssl

import javax.net.ssl.SSLEngine

trait SSLEngineFactory {
  def createServerEngine(): SSLEngine
  def createClientEngine(): SSLEngine
  def dispose(sslEngine: SSLEngine): Unit
}
