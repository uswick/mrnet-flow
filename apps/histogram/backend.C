#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>

using namespace std;

// Registers deserializers for all the standard Schemas and Operators
void registerDeserializersBackend() {
    // Schemas
    SchemaRegistry::regCreator("Record", &RecordSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);

    // Operators
    OperatorRegistry::regCreator("InMemorySource",  &InMemorySourceOperator::create);
    OperatorRegistry::regCreator("MRNetBackOut", &MRNetBEOutOperator::create);
}

// Reads a given file using a given Schema and prints the Data objects in it
static void printDataFile(const char* fName, SchemaPtr schema) {
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
SchemaPtr getSchemaBackendNode(unsigned int numFields) {

    // The value will be a record
    RecordSchemaPtr recSchema = makePtr<RecordSchema>();
    for(int i = 0 ; i < numFields ; i++) {
        SharedPtr<ScalarSchema> recScalarSchema = makePtr<ScalarSchema>(ScalarSchema::doubleT);
        recSchema->add("Rec_" + i,  recScalarSchema);
    }
    recSchema->finalize();
    return recSchema;
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

    // Run the workflow
    //drive stream from file upstream MRNet tree
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

void createSource2SinkFlowBackend(const char *outFName, int min, int max, int max_iters, SchemaPtr schema) {
    ofstream out(outFName);

    // source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        InMemorySourceOperatorConfig source(opID, InMemorySourceOperator::RAND_SRC, max_iters, min, max, schema->getConfig());
        out << source.props->tagStr();
        ++opID;

        MRNetBackendOutOperatorConfig sink(opID);
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

int __thread BE_ARG_CNT = 0 ;
char**  BE_ARGS = NULL ;

int main(int argc, char** argv) {
    #ifdef VERBOSE
    printf("[BE]: Starting backend with MRNet param initialization. arg count : %d  args : ", argc);
    for(int i = 0 ; i < argc ; i++){
        printf("[ %s ] ", argv[i]);
    }
    printf("\n");
    #endif
    //init params
    BE_ARG_CNT = argc ;
    BE_ARGS = argv ;

    const char* opConfigFName="opconfig_backend";

    //unsigned int numStreams=3;
    unsigned int numStreams=get_num_streams();

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersBackend();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getSchemaBackendNode(10);

    // Create a BE MRNet Flow
    createSource2SinkFlowBackend(opConfigFName, 10, 150, 5, fileSchema);

    // Load the flow we previously wrote to the configuration file and run it.
    FILE* opConfig = fopen(opConfigFName, "r");
    FILEStructureParser parser(opConfig, 10000);
    map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
    fclose(opConfig);

    // Show the data objects that got written to file "sink" by the flow
//    assert(outSchemas.size()==1);
//    cout << "Sink data:"<<endl;
//    cout << "-------------------------------------------"<<endl;
//    printDataFile("sink", outSchemas.begin()->second);
//    cout << "-------------------------------------------"<<endl;

    return 0;
}
