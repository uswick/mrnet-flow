#include "data.h"
#include "schema.h"
#include "operator.h"
#include <iostream>
#include <stdio.h>

using namespace std;

// Registers deserializers for all the standard Schemas and Operators
void registerDeserializers() {
  // Schemas
  SchemaRegistry::regCreator("Record", &RecordSchema::create);
  SchemaRegistry::regCreator("Tuple",  &TupleSchema::create);
  SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
  SchemaRegistry::regCreator("ExplicitKeyVal", &ExplicitKeyValSchema::create);
  
  // Operators
  OperatorRegistry::regCreator("InFile",  &InFileOperator::create);
  OperatorRegistry::regCreator("OutFile", &OutFileOperator::create);  
  OperatorRegistry::regCreator("SynchedKeyValJoin", &SynchedKeyValJoinOperator::create);  
  OperatorRegistry::regCreator("Scatter", &ScatterOperator::create);  
}

// Reads a given file using a given Schema and prints the Data objects in it
void printDataFile(const char* fName, SchemaPtr schema) {
  FILE* in = fopen(fName, "r");
  assert(in);
  unsigned int dNum=0;
  fgetc(in);
  while(!feof(in)) {
    fseek(in, -1, SEEK_CUR);
    DataPtr data = schema->deserialize(in);
    cout << dNum << ": "; data->str(cout, schema) << endl;
    cout << "-----"<<endl;
    ++dNum;
    fgetc(in);
  }
  
  fclose(in);
}

// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getSchema(const char *fName, unsigned int numStreams) {
  // The data in this file will be an ExplicitKeyValSchema
  
  // The key is a scalar integer
  SharedPtr<ScalarSchema> keyISchema = makePtr<ScalarSchema>(ScalarSchema::intT);
  SharedPtr<ScalarSchema> keyJSchema = makePtr<ScalarSchema>(ScalarSchema::intT);
  TupleSchemaPtr keySchema = makePtr<TupleSchema>();
  keySchema->add(keyISchema);
  keySchema->add(keyJSchema);
      
  // The value will be a record
  SharedPtr<ScalarSchema> recScalarSchema0 = makePtr<ScalarSchema>(ScalarSchema::stringT);
  SharedPtr<ScalarSchema> recScalarSchema1 = makePtr<ScalarSchema>(ScalarSchema::doubleT);
  
  RecordSchemaPtr recSchema = makePtr<RecordSchema>();
  recSchema->add("name",  recScalarSchema0);
  recSchema->add("valSq", recScalarSchema1);
  recSchema->finalize();
  
  ExplicitKeyValSchemaPtr keyvalSchema = makePtr<ExplicitKeyValSchema>(keySchema, recSchema);
  

  FILE* out = fopen(fName, "w");
  assert(out);
  // Loop that emits ExplicitKeyValSchema
  for(int i=0; i<3; ++i) {
    for(int subStream=0; subStream<numStreams; ++subStream) {
      ExplicitKeyValMapPtr kvMap = makePtr<ExplicitKeyValMap>();
      
      // Loop that adds key->value pairs into kvMap
      for(int j=0; j<3; ++j) {
        //cout << "1: i="<<i<<"\n";
        
        // Key of this entry
        SharedPtr<Scalar<int> > keyI = makePtr<Scalar<int> >(i);
        SharedPtr<Scalar<int> > keyJ = makePtr<Scalar<int> >(j);
        TuplePtr key = makePtr<Tuple>(keySchema);
        key->add(keyI, keySchema);
        key->add(keyJ, keySchema);
        
        // Value of this entry
        ostringstream s; s << "subStream "<<subStream<<", iter "<<i<<", "<<j;
        SharedPtr<Scalar<string> > recScalar0 = makePtr<Scalar<string> >(s.str());
        SharedPtr<Scalar<double> > recScalar1 = makePtr<Scalar<double> >(double(i*j)/10 * i*j);
        
        RecordPtr valueRec = makePtr<Record>(recSchema);
        valueRec->add("name",  recScalar0, recSchema);
        valueRec->add("valSq", recScalar1, recSchema);
        
        // Add this key->value pair to kvMap
        kvMap->add(key, valueRec);
      }
      
      // Write out the map
      keyvalSchema->serialize(kvMap, out);
    }
  }
  
  fclose(out);
  
  return keyvalSchema;
}

class Op2OpEdge {
  public:
  unsigned int fromOpID;
  unsigned int fromOpPort;
  unsigned int toOpID;
  unsigned int toOpPort;
  
  Op2OpEdge(unsigned int fromOpID, unsigned int fromOpPort, unsigned int toOpID, unsigned int toOpPort) :
    fromOpID(fromOpID), fromOpPort(fromOpPort), toOpID(toOpID), toOpPort(toOpPort)
  {}
  
/*  bool operator==(const Op2OpEdge& that);
  
  bool operator<(const Op2OpEdge& that);*/
  
  std::ostream& str(std::ostream& out) const 
  { out << "[Op2OpEdge: "<<fromOpID<<":"<<fromOpPort<<" -> "<<toOpID<<":"<<toOpPort<<"]"; return out; }
};

// Given a parser that reads a given configuration file, load it and run it.
// Returns a mapping of the IDs of sink operators (special types that produce externally-visible
// artifacts like files or sockets) to the schemas of their outputs.
std::map<unsigned int, SchemaPtr> runFlow(structureParser& parser) {  
  // Maps Operator unique IDs to pointers to the Operators themselves
  map<unsigned int, OperatorPtr> operators;
  
  // Operators that have no incoming edges. Initially filled with all operators. 
  // Then we remove all operators that have an incoming edge.
  set<unsigned int> sourceOps;
  
  // Operators that have no outgoing edges. Initially filled with all operators. 
  // Then we remove all operators that have an outgoing edge.
  set<unsigned int> sinkOps;

  // Load all the operators
  {
    propertiesPtr allOpTags = parser.nextFull();
    assert(allOpTags->name() == "Operators");
  
    for(list<propertiesPtr>::const_iterator opTag=allOpTags->getContents().begin(); opTag!=allOpTags->getContents().end(); ++opTag) {
      // Deserialize this Operator and add it to operators
      OperatorPtr newOp = OperatorRegistry::create(*opTag);
      if(operators.find(newOp->getID()) != operators.end())
      { cerr << "ERROR: operator ID "<<newOp->getID()<<" has previously been observed!"<<endl;
        cerr << "Prior operator: "; operators[newOp->getID()]->str(cerr); cerr<<endl;
        cerr << "New operator: ";   newOp->str(cerr);                     cerr<<endl;
        assert(0); }
      operators[newOp->getID()] = newOp;
      sourceOps.insert(newOp->getID());
      sinkOps.insert(newOp->getID());
    }
  }

  /*cout << "operators:"<<endl;
  for(map<unsigned int, OperatorPtr>::iterator op=operators.begin(); op!=operators.end(); ++op) {
    cout << "    "<<op->first<<": "; op->second->str(cout); cout << endl;
  }*/
  
  // Maps each operator to its outgoing and incoming edges
  map<unsigned int, list<Op2OpEdge> > outEdges;
  map<unsigned int, vector<StreamPtr> > inEdges;
  
  // Initialize inEdges to assign enough storage to each vector to hold all the 
  // inputs of its corresponding Operator
  for(map<unsigned int, OperatorPtr>::iterator op=operators.begin(); op!=operators.end(); ++op)
    if(op->second->getNumInputs()>0)
      inEdges[op->first].resize(op->second->getNumInputs());
  
  {
    propertiesPtr allStreamTags = parser.nextFull();
    assert(allStreamTags->name() == "Streams");
  
    // Load all the operator->operator streams
    for(list<propertiesPtr>::const_iterator streamTag=allStreamTags->getContents().begin(); streamTag!=allStreamTags->getContents().end(); ++streamTag) {
      assert(operators.find((*streamTag)->begin().getInt("fromOpID")) != operators.end());
      assert(operators.find((*streamTag)->begin().getInt("toOpID")) != operators.end());
      
      // Deserialize this edge and add it to outEdges and inEdges
      Op2OpEdge edge((*streamTag)->begin().getInt("fromOpID"), (*streamTag)->begin().getInt("fromOpPort"),
                     (*streamTag)->begin().getInt("toOpID"),   (*streamTag)->begin().getInt("toOpPort"));
      outEdges[edge.fromOpID].push_back(edge);
      /* // Add a NULL Stream Pointer to inEdges to ensure that the vector has space once we place the real StreamPtr in it
      unsigned int toOpPort = (*streamTag)->begin().getInt("toOpPort");
      inEdges[edge.toOpID].reserve(toOpPort);
      inEdges[edge.toOpID][toOpPort] = NULLStream;*/
      
      // Remove the edge's target Operator from sourceOps
      sourceOps.erase((unsigned int)((*streamTag)->begin().getInt("toOpID")));
      
      // Remove the edge's source Operator from sinkOps
      sinkOps.erase((unsigned int)((*streamTag)->begin().getInt("fromOpID")));
    }
  }

/*  {
    cout << "outEdges:"<<endl;
    for(map<unsigned int, list<Op2OpEdge> >::iterator out=outEdges.begin(); out!=outEdges.end(); ++out) {
      cout << "    "<<out->first<<" #="<<out->second.size()<<endl;
      for(list<Op2OpEdge>::iterator oe=out->second.begin(); oe!=out->second.end(); ++oe) {
        cout << "        "; oe->str(cout); cout << endl;
      }
    }
  }
  
  {
    cout << "inEdges:"<<endl;
    for(map<unsigned int, vector<StreamPtr> >::iterator in=inEdges.begin(); in!=inEdges.end(); ++in) {
      cout << "    "<<in->first<<" #="<<in->second.size()<<endl;
    }
  }*/
  
  // Connect Operators via Streams
  for(map<unsigned int, OperatorPtr>::iterator opIt=operators.begin(); opIt!=operators.end(); ++opIt) {
  	unsigned int opID = opIt->first;
    OperatorPtr op = opIt->second;
    
    // If this operator has any incoming streams
    if(op->getNumInputs()>0) {
      // Connect the incoming streams to op's input ports
      map<unsigned int, vector<StreamPtr> >::iterator in=inEdges.find(opID);
      assert(in != inEdges.end());
      unsigned int inPort=0;
      for(vector<StreamPtr>::iterator ie=in->second.begin(); ie!=in->second.end(); ++ie, ++inPort) {
        op->inConnect(inPort, *ie);
      }
    
    // Else, if this operator has no incoming streams
    } else 
      op->inConnectionsComplete();
    
    vector<SchemaPtr> outStreamSchemas = op->inConnectionsComplete();
    assert(op->getNumOutputs() == outStreamSchemas.size());
    if(op->getNumOutputs()>0) {
      map<unsigned int, list<Op2OpEdge> >::iterator out=outEdges.find(opID);
      assert(out != outEdges.end());
      
      // Create Streams for all the outgoing ports, connect them to the op's output ports and record them for later connection
      unsigned int outPort=0;
      assert(out->second.size() == outStreamSchemas.size());
      list<Op2OpEdge>::iterator oe=out->second.begin();
      vector<SchemaPtr>::iterator os=outStreamSchemas.begin();
      for(; os!=outStreamSchemas.end(); ++oe, ++os, ++outPort) {
        StreamPtr outStream = makePtr<Stream>(*os);
        op->outConnect(outPort, outStream);
        //cout << "inEdges[oe->toOpID].size()="<<inEdges[oe->toOpID].size()<<", oe->toOpPort="<<oe->toOpPort<<endl;
        assert(inEdges[oe->toOpID].size() > oe->toOpPort);
        inEdges[oe->toOpID][oe->toOpPort] = outStream;
      }
    }
    
    op->outConnectionsComplete();
	}

  if(sourceOps.size()==0) { cerr << "ERROR: no source operators! Need some operator to produce data."<<endl; assert(0); }
  if(sourceOps.size()>1) { cerr << "ERROR: multiple source operators not supported in sequential implementation!"<<endl; assert(0); }
  assert(operators.find(*sourceOps.begin()) != operators.end());

  SharedPtr<SourceOperator> source = dynamicPtrCast<SourceOperator>(operators[*sourceOps.begin()]);
  if(!source) { cerr << "ERROR: source operator does not derive from SourceOperator! operator="; operators[*sourceOps.begin()]->str(cerr); cerr<<endl; assert(0); }

  // Run the workflow, with the source emitting data and other operators receiving and propagating it
  source->driver();
  
  // The source has now completed and streamFinished() tokens have been propagated along all streams.
  
  // Collect the output schemas of the sink operators
  map<unsigned int, SchemaPtr> outSchemas;
  for(set<unsigned int>::iterator op=sinkOps.begin(); op!=sinkOps.end(); ++op) {
    OutFileOperatorPtr outFile = dynamicPtrCast<OutFileOperator>(operators[*op]);
    if(outFile) {
      assert(outFile->getInStreams().size()==1);
      outSchemas[*op] = (*outFile->getInStreams().begin())->getSchema();
    } 
  }
  
  // We now shut down by destroying all Streams and Operators.
  return outSchemas;
}

void createSource2SinkFlow(const char* outFName, const char* sourceFName, const char* sinkFName, SchemaPtr schema) {
  ofstream out(outFName);
  
  // source -> sink
  
  {
    unsigned int opID=0;
    properties operators("Operators");
    out << operators.enterStr();
    
    InFileOperatorConfig source(opID, sourceFName, schema->getConfig()); 
    out << source.props->tagStr();
    ++opID;

    OutFileOperatorConfig sink(opID, sinkFName); 
    out << sink.props->tagStr();

    out << operators.exitStr();
  }
  
  {
    unsigned int opID=0;
    properties streams("Streams");
    out << streams.enterStr();

    // source:out:0 --> sink:in:0
    StreamConfig source2scatter(opID, 0, opID+1, 0);
    out << source2scatter.props.tagStr();

    out << streams.exitStr();
  }
}

void createScatterJoinFlow(const char* outFName, const char* sourceFName, const char* sinkFName, SchemaPtr schema, unsigned int numStreams) {
  ofstream out(outFName);
  
  //                   ->
  // source -> scatter -> join   -> sink
  //                   ->
  
  {
    unsigned int opID=0;
    properties operators("Operators");
    out << operators.enterStr();
    
    InFileOperatorConfig source(opID, sourceFName, schema->getConfig()); 
    out << source.props->tagStr();
    ++opID;

    ScatterOperatorConfig scatter(/*numOutputs*/ numStreams, opID); 
    out << scatter.props->tagStr();
    ++opID;

    /* // Create a single file output operator for each stream
    for(int s=0; s<numStreams; ++s) {
      OutFileOperatorConfig sink(opID, string(txt()<<sinkFName<<"."<<s).c_str()); 
      out << sink.props->tagStr();
      ++opID;
    }*/
    SynchedKeyValJoinOperatorConfig join(/*numInputs*/ numStreams, opID); 
    out << join.props->tagStr();
    ++opID;
    
    OutFileOperatorConfig sink(opID, sinkFName); 
    out << sink.props->tagStr();

    out << operators.exitStr();
  }
  
  {
    unsigned int opID=0;
    properties streams("Streams");
    out << streams.enterStr();

    // source:out:0 --> scatter:in:0
    StreamConfig source2scatter(opID, 0, opID+1, 0);
    out << source2scatter.props.tagStr();
    ++opID;
    
    for(int s=0; s<numStreams; ++s) {
      /* // scatter:out:s --> sink_s:in:0
      StreamConfig scatter2sink(opID, s, opID+1+s, 0);
      out << scatter2sink.props.tagStr(); */
      // scatter:out:s --> join:in:s
      StreamConfig scatter2join(opID, s, opID+1, s);
      out << scatter2join.props.tagStr();
    }
    //opID+=numStreams;
    ++opID;
    
    // source:out:0 --> sink:in:0
    StreamConfig join2sink(opID, 0, opID+1, 0);
    out << join2sink.props.tagStr();

    out << streams.exitStr();
  }
}

int main(int argc, char** argv) {
  const char* opConfigFName="opconfig";
  if(argc>1) opConfigFName = argv[1];
  int flowChoice = 1;
  if(argc>2) flowChoice = atoi(argv[2]);

  unsigned int numStreams=3;

  // First, register the deserializers for all the Schemas and Operators that may be used
  registerDeserializers();

  // The flows we'll run get their input data from a file, so initialize the file to hold some data
  SchemaPtr fileSchema = getSchema("source", numStreams);
  
  // Create a Flow and write it out to a configuration file. 
  if(flowChoice==0) 
    // This flow reads data objects from file "source" and writes them out to file "sink"
    createSource2SinkFlow(opConfigFName, "source", "sink", fileSchema);
  else if(flowChoice==1)
    // This flow reads KeyValMap objects from file "source", scatters them to multiple streams,
    // joins objects on those streams based on common keys and writes the joined ExplicitKeyValMaps 
    // out to file "sink"  
    createScatterJoinFlow(opConfigFName, "source", "sink", fileSchema, numStreams);
  
  // Show the input data in file "source"
  cout << "Source data:"<<endl;
  cout << "-------------------------------------------"<<endl;
  printDataFile("source", fileSchema);
  cout << "-------------------------------------------"<<endl;

  // Load the flow we previously wrote to the configuration file and run it.
  FILE* opConfig = fopen(opConfigFName, "r");
  FILEStructureParser parser(opConfig, 10000);
  map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
  fclose(opConfig);

  // Show the data objects that got written to file "sink" by the flow
  assert(outSchemas.size()==1);
  cout << "Sink data:"<<endl;
  cout << "-------------------------------------------"<<endl;
  printDataFile("sink", outSchemas.begin()->second);
  cout << "-------------------------------------------"<<endl;

  return 0;
}