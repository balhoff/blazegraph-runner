# blazegraph-runner
`blazegraph-runner` provides a simple command-line wrapper for the [Blazegraph](https://www.blazegraph.com) open source 
RDF database. It provides operations on an "offline" database, so that you can easily load data or execute queries against 
a Blazegraph journal file, without needing to run it as an HTTP SPARQL server.

## Usage

```
Usage

 blazegraph-runner [options] command [command options]

Options

   --informat   : Input format
   --journal    : Blazegraph journal file
   --outformat  : Output format
   --properties : Blazegraph properties file

Commands

   construct <query> <output> : SPARQL construct

   dump [command options] <output> : Dump Blazegraph database to an RDF file
      --graph : Named graph to load triples into

   load [command options] <dataFiles> ... : Load triples
      --base=STRING
      --graph              : Named graph to load triples into
      --use-ontology-graph

   reason [command options] : Materialize inferences
      --append-graph-name=STRING : If a target-graph is not provided, append this text to the end of source graph name to use as target graph for inferred statements.
      --merge-sources            : Merge all selected source graphs into one set of statements before reasoning. Inferred statements will be stored in provided `target-graph`, or else in the default graph. If `merge-sources` is false (default), source graphs will be reasoned separately and in parallel.
      --ontology                 : Ontology to use as rule source. If the passed value is a valid filename, the ontology will be read from the file. Otherwise, if the value is an ontology IRI, it will be loaded from the database if such a graph exists, or else, from the web.
      --parallelism=NUM          : Maximum graphs to simultaneously either read from database or run reasoning on.
      --rules-file               : Reasoning rules in Jena syntax.
      --source-graphs            : Space-separated graph IRIs on which to perform reasoning (must be passed as one shell argument).
      --source-graphs-query      : File name or query text of SPARQL select used to obtain graph names on which to perform reasoning. The query must return a column named `source_graph`.
      --target-graph             : Named graph to store inferred statements.

   select <query> <output> : SPARQL select

   update <update> : SPARQL update
```

## Load

Load RDF data from files into a Blazegraph journal. A list of files or folders can be passed to the command; folders will 
be recursively searched for data files.

```
blazegraph-runner load --journal=blazegraph.jnl --graph="http://example.org/mydata" --informat=rdfxml mydata1.rdf mydata2.rdf
```

If your data files are OWL ontologies, `blazegraph-runner` can efficiently search within each file to find the ontology IRI 
if you want to use it as the target named graph:

```
blazegraph-runner load --journal=blazegraph.jnl --use-ontology-graph=true --informat=rdfxml go.owl
```

If you set `--use-ontology-graph=true` and also provide a value for `--graph`, the `--graph` will be used as a fallback value 
in the case that an ontology IRI is not found.

## Dump

Export RDF data from a Blazegraph journal to a file. If a value for `--graph` is provided, only data from that graph is 
exported. If `--graph` is not provided, data from the default graph will be exported. *In the future this command should 
be extended to dump all graphs to separate file or dump all data to a quad format.*

```
blazegraph-runner dump --journal=blazegraph.jnl --graph="http://example.org/mydata" --outformat=turtle mydata.ttl
```

## Select

Query a Blazegraph journal using SPARQL SELECT:

```
blazegraph-runner select --journal=blazegraph.jnl --outformat=tsv mydata.tsv
```


