package org.renci.blazegraph

import java.io.File

import scala.io.Source

import org.backuity.clist._
import org.openrdf.query.QueryLanguage

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Update extends Command(description = "SPARQL update") with Common {

  var updateFile = arg[File](name = "update")

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val mutationCounter = new MutationCounter()
    blazegraph.addChangeLog(mutationCounter)
    blazegraph.begin()
    val update = blazegraph.prepareUpdate(QueryLanguage.SPARQL, Source.fromFile(updateFile, "utf-8").mkString)
    update.execute()
    val mutations = mutationCounter.mutationCount
    blazegraph.commit()
    blazegraph.removeChangeLog(mutationCounter)
    logger.info(s"$mutations changes")
  }

}
