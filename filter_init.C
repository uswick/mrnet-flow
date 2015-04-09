#include <unistd.h>
#include "filter_init.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"

//SharedPtr<SourceOperator> source_op;
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
    OperatorRegistry::regCreator("SynchedKeyValJoin", &SynchedKeyValJoinOperator::create);
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

void createFilterSource2Join2OutFlow(const char* outFName, SchemaPtr schema, unsigned int numStreams) {
    ofstream out(outFName);

    // MRNet filter source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        MRNetFilterSourceOperatorConfig source(opID, numStreams, schema->getConfig());
        out << source.props->tagStr();
        ++opID;

        SynchedKeyValJoinOperatorConfig join(/*numInputs*/ numStreams, opID);
        out << join.props->tagStr();
        ++opID;

        MRNetFilterOutOperatorConfig sink(opID);
        out << sink.props->tagStr();

        out << operators.exitStr();
    }

    {
        unsigned int opID=0;
        properties streams("Streams");
        out << streams.enterStr();

        //create streams to join source with gather operator
        int s=0;
        for(s=0; s<numStreams; ++s) {
            /* // scatter:out:s --> sink_s:in:0
            StreamConfig scatter2sink(opID, s, opID+1+s, 0);
            out << scatter2sink.props.tagStr(); */
            // scatter:out:s --> join:in:s
            StreamConfig source2join(opID, s, opID+1, s);
            out << source2join.props.tagStr();
        }
        //opID+=numStreams;
        ++opID;

        // join:out:0 --> sink:in:0
        StreamConfig join2sink(opID, 0, opID+1, 0);
        out << join2sink.props.tagStr();

        out << streams.exitStr();
    }
}



glst_t filter_flow_init(){
//    const char* opConfigFName="opconfig_filter";
    Flow_Init(0, NULL);
    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFilter();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getInputSchemaFilterNode();

    // Create a Flow and write it out to a configuration file.
//    createFilterSource2OutFlow(opConfigFName , fileSchema);
//    createFilterSource2Join2OutFlow(opConfigFName, fileSchema,  get_num_streams());
    createFilterSource2Join2OutFlow(CONFIG_FILTER, fileSchema, NUM_STREAMS);

    FILE* opConfig = fopen(CONFIG_FILTER, "r");
    FILEStructureParser parser(opConfig, 10000);

    glst_t filter_inf ;
    getFlowSource(parser, filter_inf);

    fprintf(stdout, "[Filter]: initialization complete PID : %d thread ID : %lu \n", getpid(), pthread_self());

    return filter_inf;
}
