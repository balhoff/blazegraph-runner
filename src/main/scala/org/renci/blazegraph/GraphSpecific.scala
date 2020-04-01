package org.renci.blazegraph

import org.backuity.clist._

trait GraphSpecific extends Command {

  var graphOpt = opt[Option[String]](name = "graph", description = "Named graph to load triples into (if not provided, file URL will be used)")

}