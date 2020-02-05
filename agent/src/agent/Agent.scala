package agent

import java.lang.instrument.Instrumentation

/**
  * mill agent.compile
  * jar cmf ./agent/META-INF/MANIFEST.MF Agent.jar ./out/agent/compile/dest/classes/agent/Agent.class
  */
object Agent {
  private var instrumentation: Instrumentation = null
  def premain(args: String, inst: Instrumentation): Unit = {
    instrumentation = inst
  }
  def getObjectSize(o: Any) = instrumentation.getObjectSize(o)
}
