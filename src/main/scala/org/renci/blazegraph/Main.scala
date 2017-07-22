package org.renci.blazegraph

import org.backuity.clist._

object Main extends App {

  Cli.parse(args).withProgramName("blazegraph-runner").withCommands(Load, Dump, Select, Construct, Update).foreach(_.run)

}