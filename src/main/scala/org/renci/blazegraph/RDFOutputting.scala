
package org.renci.blazegraph
import org.openrdf.query.QueryLanguage
import org.openrdf.rio.RDFWriter
import org.openrdf.rio.ntriples.NTriplesWriter
import org.openrdf.rio.rdfxml.RDFXMLWriter
import org.openrdf.rio.turtle.TurtleWriter
import java.io.OutputStream

trait RDFOutputting extends Common {

  def createOutputWriter(out: OutputStream): RDFWriter = outformat.getOrElse("turtle") match {
    case "turtle"   => new TurtleWriter(out)
    case "rdfxml"   => new RDFXMLWriter(out)
    case "ntriples" => new NTriplesWriter(out)
    case other      => throw new IllegalArgumentException(s"Invalid SPARQL construct output format: $other")
  }

}