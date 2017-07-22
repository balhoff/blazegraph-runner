package org.renci.blazegraph

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

import scala.io.Source

import org.backuity.clist._
import org.openrdf.query.QueryLanguage

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

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
