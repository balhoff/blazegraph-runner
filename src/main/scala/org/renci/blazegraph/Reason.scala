package org.renci.blazegraph

import java.io.File

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.bigdata.rdf.sail.{BigdataSailRepository, BigdataSailRepositoryConnection}
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.sys.JenaSystem
import org.backuity.clist._
import org.geneontology.jena.OWLtoRules
import org.geneontology.rules.engine.{RuleEngine, Triple}
import org.geneontology.rules.util.{Bridge => RulesBridge}
import org.geneontology.whelk.{AtomicConcept, BuiltIn, ConceptAssertion, ConceptInclusion, ExistentialRestriction, Individual, Nominal, Reasoner, Role, RoleAssertion, Bridge => WhelkBridge}
import org.openrdf.model.impl.URIImpl
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Statement, URI, ValueFactory}
import org.openrdf.query.QueryLanguage
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.RioRDFXMLDocumentFormatFactory
import org.semanticweb.owlapi.model.parameters.Imports
import org.semanticweb.owlapi.model.{IRI, OWLOntology, OWLOntologyLoaderConfiguration}
import org.semanticweb.owlapi.rio.{RioMemoryTripleSource, RioParserImpl}
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.control.NonFatal

object Reason extends Command(description = "Materialize inferences") with Common {

  var targetGraph = opt[Option[String]](description = "Named graph to store inferred statements.")
  var appendGraphName = opt[String](default = "_inferred", description = "If a target-graph is not provided, append this text to the end of source graph name to use as target graph for inferred statements.")
  var mergeSources = opt[Boolean](default = false, description = "Merge all selected source graphs into one set of statements before reasoning. Inferred statements will be stored in provided `target-graph`, or else in the default graph. If `merge-sources` is false (default), source graphs will be reasoned separately and in parallel.")
  var ontology = opt[Option[String]](description = "Ontology to use as rule source. If the passed value is a valid filename, the ontology will be read from the file. Otherwise, if the value is an ontology IRI, it will be loaded from the database if such a graph exists, or else, from the web.")
  var rulesFile = opt[Option[File]](description = "Reasoning rules in Jena syntax.")
  var parallelism = opt[Int](default = Math.max(Runtime.getRuntime().availableProcessors / 2, 2), description = "Maximum graphs to simultaneously either read from database or run reasoning on.")
  var sourceGraphsQuery = opt[Option[String]](description = "File name or query text of SPARQL select used to obtain graph names on which to perform reasoning. The query must return a column named `source_graph`.")
  var sourceGraphs = opt[Option[String]](description = "Space-separated graph IRIs on which to perform reasoning (must be passed as one shell argument).")
  var reasonerChoice = opt[String](name = "reasoner", default = "arachne", description = "Reasoner choice: 'arachne' (default) or 'whelk'")

  private val ProvDerivedFrom = new URIImpl("http://www.w3.org/ns/prov#wasDerivedFrom")

  private def computedTargetGraph(graph: String): Option[String] = targetGraph.orElse {
    if (mergeSources) None
    else Option(inferredGraphName(graph))
  }

  private def inferredGraphName(graph: String): String = s"$graph$appendGraphName"

  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()

    val maybeOnt = ontology.flatMap(tryLoadOntologyFromFile)
      .orElse(ontology.flatMap(tryLoadOntologyFromDatabase(_, blazegraph.getRepository)))
      .orElse(ontology.flatMap(tryLoadOntologyFromWeb))
    val factory = blazegraph.getValueFactory
    val reasoner = reasonerChoice.toLowerCase match {
      case "arachne" => new ArachneReasoner(maybeOnt, rulesFile.map(f => io.Source.fromFile(f, "utf-8").mkString), factory)
      case "whelk"   => new WhelkReasoner(maybeOnt.getOrElse(OWLManager.createOWLOntologyManager().createOntology()), factory)
      case _         => throw new IllegalArgumentException(s"Invalid reasoner choice: $reasonerChoice")
    }
    implicit val system = ActorSystem("Reason")
    implicit val materializer = ActorMaterializer()

    var sourceGraphNames = sourceGraphs.map(_.split(" ", -1)).toList.flatten
    sourceGraphsQuery.foreach { sgQuery =>
      val sourcesQueryString = if (new File(sgQuery).exists) io.Source.fromFile(sgQuery, "utf-8").mkString else sgQuery
      val query = blazegraph.prepareTupleQuery(QueryLanguage.SPARQL, sourcesQueryString)
      val sourcesQueryResult = query.evaluate()
      while (sourcesQueryResult.hasNext) {
        val bindingSet = sourcesQueryResult.next()
        if (bindingSet.hasBinding("source_graph")) sourceGraphNames = bindingSet.getValue("source_graph").stringValue :: sourceGraphNames
        else {
          scribe.error(s"The SPARQL query for source graphs must return a binding for the variable 'source_graph'. Found instead: ${bindingSet.getBindingNames.asScala.mkString(",")}")
          system.terminate()
          System.exit(1)
        }
      }
      sourcesQueryResult.close()
    }
    scribe.debug(s"Reasoning on source graphs: \n${sourceGraphNames.mkString("\n")}")

    val sourceGraphGroups = if (mergeSources) List(sourceGraphNames -> computedTargetGraph(sourceGraphNames.head))
    else sourceGraphNames.map(g => List(g) -> computedTargetGraph(g))

    // Now process each graph group in 3 stages:
    // (1) load graph group statements from database
    // (2) run reasoner over statements
    // (3) insert inferred statements into database
    // Each of these will happen in parallel up to the maximum 'parallelism' value, but early stages will wait
    // to start a new graph group if downstream stages are busy.
    blazegraph.begin()
    val done = Source(sourceGraphGroups)
      .mapAsyncUnordered(parallelism) {
        case (graphs, targetGraphName) =>
          scribe.debug(s"Loading from $graphs for target $targetGraphName")
          val sourceGraphs = graphs.map(new URIImpl(_))
          val maybeTargetGraph = targetGraphName.map(new URIImpl(_))
          statementsForGraphs(sourceGraphs, blazegraph.getRepository).map((_, sourceGraphs, maybeTargetGraph))
      }
      .mapAsyncUnordered(parallelism) {
        case (statements, sourceGraphs, maybeTargetGraph) => Future {
          val factory = blazegraph.getValueFactory
          val provenanceStatements = for {
            sourceGraph <- sourceGraphs
            targetGraph <- maybeTargetGraph
          } yield factory.createStatement(targetGraph, ProvDerivedFrom, sourceGraph)
          scribe.debug(s"Reasoning for $maybeTargetGraph")
          val inferred = reasoner.computeInferences(statements)
          val result = (inferred ++ provenanceStatements, maybeTargetGraph)
          scribe.debug(s"Done reasoning $maybeTargetGraph")
          result
        }
      }
      .runForeach {
        case (statements, Some(targetGraph)) =>
          blocking {
            scribe.debug(s"Inserting result into $targetGraph")
            blazegraph.add(statements.asJava, targetGraph)
          }
      }
    Await.ready(done, Duration.Inf).onComplete {
      case Failure(e) =>
        blazegraph.rollback()
        e.printStackTrace()
        system.terminate()
        System.exit(1)
      case _          => {
        blazegraph.commit()
        system.terminate()
      }
    }
  }

  private def tryLoadOntologyFromFile(filename: String): Option[OWLOntology] = try {
    val file = new File(filename)
    if (file.exists) Option(OWLManager.createOWLOntologyManager().loadOntologyFromOntologyDocument(file)) else None
  } catch {
    case NonFatal(e) => None
  }

  private def tryLoadOntologyFromDatabase(graphName: String, repository: BigdataSailRepository): Option[OWLOntology] = {
    val manager = OWLManager.createOWLOntologyManager()
    val statements = Await.result(statementsForGraphs(List(new URIImpl(graphName)), repository), Duration.Inf)
    ontologyFromStatements(statements)
  }

  private def ontologyFromStatements(statements: Set[Statement]): Option[OWLOntology] = try {
    val manager = OWLManager.createOWLOntologyManager()
    if (statements.nonEmpty) {
      val source = new RioMemoryTripleSource(statements.asJava)
      val parser = new RioParserImpl(new RioRDFXMLDocumentFormatFactory())
      val newOnt = manager.createOntology()
      //TODO what will happen with imports? Ignored, or loaded from web??
      parser.parse(source, newOnt, new OWLOntologyLoaderConfiguration())
      Option(newOnt)
    } else None
  } catch {
    case NonFatal(e) => None
  }

  private def tryLoadOntologyFromWeb(ontIRI: String): Option[OWLOntology] = try {
    val manager = OWLManager.createOWLOntologyManager()
    Option(manager.loadOntology(IRI.create(ontIRI)))
  } catch {
    case NonFatal(e) => None
  }

  private def statementsForGraphs(graphs: Seq[URI], repository: BigdataSailRepository): Future[Set[Statement]] = Future {
    blocking {
      val connection = repository.getReadOnlyConnection
      val result = connection.getStatements(null, null, null, false, graphs: _*)
      var statements = Set.empty[Statement]
      while (result.hasNext) statements += result.next
      result.close()
      connection.close()
      statements
    }
  }

  private sealed trait BGReasoner {

    def computeInferences(statements: Set[Statement]): Set[Statement]

  }

  private class ArachneReasoner(ontology: Option[OWLOntology], rules: Option[String], factory: ValueFactory) extends BGReasoner {

    private val ontRules = ontology.toSet[OWLOntology].flatMap(ont => RulesBridge.rulesFromJena(OWLtoRules.translate(ont, Imports.INCLUDED, true, true, false, true)).toSet)
    private val extraRules = rules.toSet[String].flatMap(rs => RulesBridge.rulesFromJena(Rule.parseRules(rs).asScala).toSet)
    private val allRules = ontRules ++ extraRules
    private val arachne = new RuleEngine(allRules, false)

    def computeInferences(statements: Set[Statement]): Set[Statement] = {
      val triples = statements.map(ArachneBridge.createTriple)
      val wm = arachne.processTriples(triples)
      val inferred: Set[Triple] = wm.facts.toSet -- wm.asserted
      inferred.map(ArachneBridge.createStatement(factory, _))
    }

  }

  private class WhelkReasoner(ontology: OWLOntology, factory: ValueFactory) extends BGReasoner {

    private val tbox = WhelkBridge.ontologyToAxioms(ontology)
    private val whelk = Reasoner.assert(tbox)
    private val tboxClassAssertions = whelk.classAssertions
    private val tboxRoleAssertions = whelk.roleAssertions
    //TODO this should possibly be part of the whelk API
    //ObjectProperty declarations are needed for correct parsing of OWL properties from RDF
    private val allRoles = tbox.flatMap(_.signature).collect { case role: Role => role }
    private val propertyDeclarations = allRoles.map(r => factory.createStatement(factory.createURI(r.id), RDF.TYPE, factory.createURI(OWLRDFVocabulary.OWL_OBJECT_PROPERTY.getIRI.toString)))

    def computeInferences(statements: Set[Statement]): Set[Statement] = {
      val statementsWithoutImports = statements.filterNot(_.getPredicate.stringValue == OWLRDFVocabulary.OWL_IMPORTS.getIRI.toString)
      ontologyFromStatements(statementsWithoutImports ++ propertyDeclarations) match {
        case Some(ont) =>
          val axioms = WhelkBridge.ontologyToAxioms(ont).collect { case ci: ConceptInclusion => ci }
          val assertedClassAssertions = (for {
            ConceptInclusion(Nominal(ind), ac @ AtomicConcept(_)) <- axioms
          } yield ConceptAssertion(ac, ind))
          val assertedRoleAssertions = (for {
            ConceptInclusion(Nominal(subject), ExistentialRestriction(role @ Role(_), Nominal(target))) <- axioms
          } yield RoleAssertion(role, subject, target))
          val updated = Reasoner.assert(axioms, whelk)
          val newClassAssertions = (updated.classAssertions -- assertedClassAssertions -- tboxClassAssertions)
            .filterNot(_.concept == BuiltIn.Top)
          val newRoleAssertions = updated.roleAssertions -- assertedRoleAssertions -- tboxRoleAssertions
          newClassAssertions.flatMap(ca => classAssertionToStatement(ca, factory).toSet) ++ newRoleAssertions.map(roleAssertionToStatement(_, factory))
        case None      =>
          scribe.error("Couldn't create OWL ontology from RDF statements")
          Set.empty
      }
    }

    private def classAssertionToStatement(ca: ConceptAssertion, factory: ValueFactory): Option[Statement] = ca match {
      case ConceptAssertion(AtomicConcept(c), Individual(ind)) => Some(factory.createStatement(factory.createURI(ind), RDF.TYPE, factory.createURI(c)))
      case _                                                   => None
    }

    private def roleAssertionToStatement(ra: RoleAssertion, factory: ValueFactory): Statement = factory.createStatement(factory.createURI(ra.subject.id), factory.createURI(ra.role.id), factory.createURI(ra.target.id))

  }

}