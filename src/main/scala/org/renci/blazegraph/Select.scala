package org.renci.blazegraph

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import org.backuity.clist._
import org.openrdf.query.QueryLanguage
import org.openrdf.query.resultio.TupleQueryResultWriter
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter

import scala.io.Source

object Select extends Command(description = "SPARQL select") with Common {

  var queryFile = arg[File](name = "query")
  var output = arg[File]()

  def createOutputWriter(out: OutputStream): TupleQueryResultWriter = outformat.getOrElse("tsv").toLowerCase match {
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
