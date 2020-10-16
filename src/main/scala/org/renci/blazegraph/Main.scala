package org.renci.blazegraph

import org.backuity.clist._

object Main extends App {

  try {
    Cli.parse(args).withProgramName("blazegraph-runner").withCommands(Load, Dump, Select, Construct, Update, Reason).foreach(_.run())
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }

}