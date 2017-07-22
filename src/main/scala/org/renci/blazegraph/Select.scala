package org.renci.blazegraph

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

import scala.io.Source

import org.backuity.clist._
import org.openrdf.query.QueryLanguage
import org.openrdf.query.resultio.TupleQueryResultWriter
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection

object Select extends Command(description = "SPARQL select") with Common {

  var queryFile = arg[File](name = "query")
  var output = arg[File]()

  def createOutputWriter(out: OutputStream): TupleQueryResultWriter = outformat.getOrElse("tsv") match {
    case "tsv"  => new SPARQLResultsTSVWriter(out)
    case "xml"  => new SPARQLResultsXMLWriter(out)
    case "json" => new SPARQLResultsJSONWriter(out)
    case other  => throw new IllegalArgumentException(s"Invalid SPARQL select output format: $other")
  }

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    val query = blazegraph.prepareTupleQuery(QueryLanguage.SPARQL, Source.fromFile(queryFile, "utf-8").mkString)
    val queryOutput = new BufferedOutputStream(new FileOutputStream(output))
    query.evaluate(createOutputWriter(queryOutput))
    queryOutput.close()
  }

}
