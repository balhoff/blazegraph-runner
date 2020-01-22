package org.renci.blazegraph

import java.io.File

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import org.backuity.clist._
import org.openrdf.query.QueryLanguage

import scala.io.Source

object Update extends Command(description = "SPARQL update") with Common {

  var updateFile = arg[File](name = "update")

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    val update = blazegraph.prepareUpdate(QueryLanguage.SPARQL, Source.fromFile(updateFile, "utf-8").mkString)
    update.execute()
    blazegraph.commit()
    val mutations = mutationCounter.mutationCount
    blazegraph.removeChangeLog(mutationCounter)
    scribe.info(s"$mutations changes")
  }

}
