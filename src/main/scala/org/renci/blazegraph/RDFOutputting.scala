package org.renci.blazegraph

import java.io.OutputStream
import org.openrdf.rio.RDFWriter
import org.openrdf.rio.nquads.NQuadsWriter
import org.openrdf.rio.ntriples.NTriplesWriter
import org.openrdf.rio.rdfxml.RDFXMLWriter
import org.openrdf.rio.turtle.TurtleWriter

trait RDFOutputting extends Common {

  def createOutputWriter(out: OutputStream): RDFWriter = outformat.getOrElse("turtle").toLowerCase match {
    case "turtle" | "ttl"         => new TurtleWriter(out)
    case "rdfxml" | "rdf-xml"     => new RDFXMLWriter(out)
    case "ntriples" | "n-triples" => new NTriplesWriter(out)
    case "nquads" | "n-quads"     => new NQuadsWriter(out)
    case other                    => throw new IllegalArgumentException(s"Invalid RDF output format: $other")
  }

}
