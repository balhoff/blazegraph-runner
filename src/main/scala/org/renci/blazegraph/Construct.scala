package org.renci.blazegraph

import java.io.{BufferedOutputStream, File, FileOutputStream}

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import org.backuity.clist._
import org.openrdf.query.QueryLanguage

import scala.io.Source

object Construct extends Command(description = "SPARQL construct") with Common with RDFOutputting {

  var queryFile = arg[File](name = "query")
  var output = arg[File]()

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val query = blazegraph.prepareGraphQuery(QueryLanguage.SPARQL, Source.fromFile(queryFile, "utf-8").mkString)
    val queryOutput = new BufferedOutputStream(new FileOutputStream(output))
    query.evaluate(createOutputWriter(queryOutput))
    queryOutput.close()
  }

}
