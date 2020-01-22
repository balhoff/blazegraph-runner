package org.renci.blazegraph

import org.geneontology.rules.engine.{BlankNode, ConcreteNode, Triple}
import org.openrdf.model._

object ArachneBridge {

  def createStatement(factory: ValueFactory, triple: Triple): Statement = {
    val subject: Resource = resourceFromArachne(factory, triple.s)
    val predicate: URI = uriFromArachne(factory, triple.p)
    val obj: Value = valueFromArachne(factory, triple.o)
    factory.createStatement(subject, predicate, obj)
  }

  def createTriple(statement: Statement): Triple = {
    Triple(resourceToArachne(statement.getSubject), uriToArachne(statement.getPredicate), valueToArachne(statement.getObject))
  }

  def uriFromArachne(factory: ValueFactory, uri: org.geneontology.rules.engine.URI): URI = factory.createURI(uri.uri)

  def uriToArachne(uri: URI): org.geneontology.rules.engine.URI = org.geneontology.rules.engine.URI(uri.stringValue)

  def resourceFromArachne(factory: ValueFactory, resource: org.geneontology.rules.engine.Resource): Resource = resource match {
    case uri @ org.geneontology.rules.engine.URI(_) => uriFromArachne(factory, uri)
    case BlankNode(id)                              => factory.createBNode(id)
  }

  def resourceToArachne(resource: Resource): org.geneontology.rules.engine.Resource = resource match {
    case uri: URI     => uriToArachne(uri)
    case bnode: BNode => BlankNode(bnode.getID)
  }

  def valueFromArachne(factory: ValueFactory, node: ConcreteNode): Value = node match {
    case org.geneontology.rules.engine.URI(value)                                                    => factory.createURI(value)
    case BlankNode(id)                                                                               => factory.createBNode(id)
    case org.geneontology.rules.engine.Literal(value, _, Some(lang))                                 => factory.createLiteral(value, lang)
    case org.geneontology.rules.engine.Literal(value, uri @ org.geneontology.rules.engine.URI(_), _) => factory.createLiteral(value, uriFromArachne(factory, uri))
  }

  def valueToArachne(value: Value): ConcreteNode = value match {
    case bnode: BNode     => BlankNode(bnode.getID)
    case uri: URI         => uriToArachne(uri)
    case literal: Literal =>
      val datatype = if (literal.getDatatype == null) org.geneontology.rules.engine.URI("http://www.w3.org/2001/XMLSchema#string") else uriToArachne(literal.getDatatype)
      org.geneontology.rules.engine.Literal(literal.stringValue(), datatype, Option(literal.getLanguage))
  }

}