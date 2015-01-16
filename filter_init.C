#include "filter_init.h"
#include "mrnet_operator.h"

SharedPtr<SourceOperator> source_op;
SharedPtr<MRNetFilterOutOperator> out_op_filter;
map<unsigned int, SchemaPtr> output_schemas;


static void registerDeserializersFilter() {
    // Schemas
    SchemaRegistry::regCreator("Record", &RecordSchema::create);
    SchemaRegistry::regCreator("Tuple",  &TupleSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
    SchemaRegistry::regCreator("ExplicitKeyVal", &ExplicitKeyValSchema::create);

    // Operators
    OperatorRegistry::regCreator("MRNetFilterOut", &MRNetFilterOutOperator::create);
    OperatorRegistry::regCreator("MRNetFilterSource", &MRNetFilterSourceOperator::create);
}

// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getInputSchemaFilterNode() {
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
SharedPtr<SourceOperator>& getFlowSource(structureParser& parser) {
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

    source_op = dynamicPtrCast<SourceOperator>(operators[*sourceOps.begin()]);
    if(!source_op) { cerr << "ERROR: source operator does not derive from SourceOperator! operator="; operators[*sourceOps.begin()]->str(cerr); cerr<<endl; assert(0); }

    //get output schema for  debugging
    for(set<unsigned int>::iterator op=sinkOps.begin(); op!=sinkOps.end(); ++op) {
        out_op_filter = dynamicPtrCast<MRNetFilterOutOperator>(operators[*op]);
        if(out_op_filter) {
            assert(out_op_filter->getInStreams().size()==1);
            output_schemas[*op] = (*out_op_filter->getInStreams().begin())->getSchema();
        }
    }

    return source_op;
}

SharedPtr<MRNetFilterOutOperator>& getOutOperator(){
    return out_op_filter;
}

map<unsigned int, SchemaPtr>& getOutputSchemas(){
    return output_schemas;
}

void createFilterSource2OutFlow(const char* outFName, SchemaPtr schema) {
    ofstream out(outFName);

    // MRNet filter source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        MRNetFilterSourceOperatorConfig source(opID, 1, schema->getConfig());
        out << source.props->tagStr();
        ++opID;

        MRNetFilterOutOperatorConfig sink(opID);
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


SharedPtr<SourceOperator>& filter_flow_init(){
    const char* opConfigFName="opconfig_filter";

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFilter();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getInputSchemaFilterNode();

    // Create a Flow and write it out to a configuration file.
    createFilterSource2OutFlow(opConfigFName , fileSchema);

    FILE* opConfig = fopen(opConfigFName, "r");
    FILEStructureParser parser(opConfig, 10000);

    SharedPtr<SourceOperator> src_op = getFlowSource(parser);

    return src_op;
}