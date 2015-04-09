#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>

using namespace std;

//int __thread BE_ARG_CNT = 0 ;
//char**  BE_ARGS = NULL ;

// Registers deserializers for all the standard Schemas and Operators
static void registerDeserializersFront() {
    // Schemas
    SchemaRegistry::regCreator("Record", &RecordSchema::create);
    SchemaRegistry::regCreator("Tuple",  &TupleSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
    SchemaRegistry::regCreator("ExplicitKeyVal", &ExplicitKeyValSchema::create);

    // Operators
    OperatorRegistry::regCreator("MRNetFrontSource",  &MRNetFESourceOperator::create);
    OperatorRegistry::regCreator("OutFile", &OutFileOperator::create);
}


// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getSchemaCommon() {
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

SchemaPtr getAggregate_Schema(int numStreams){
    KeyValSchemaPtr keyValSchema = dynamicPtrCast<KeyValSchema>(getSchemaCommon());

    // Generate the schema for tuples of values from the streams
    TupleSchemaPtr valTupleSchema = makePtr<TupleSchema>();
    for(int i = 0 ; i < numStreams  ; ++i) {
        valTupleSchema->add(keyValSchema->getValue());
    }

    // Generate the schema for the output of this operator
    ExplicitKeyValSchemaPtr outputKeyValSchema = makePtr<ExplicitKeyValSchema>(keyValSchema->getKey(), valTupleSchema);

    return outputKeyValSchema;
}


void createSource2SinkFlowFront(const char *outFName, const char *sinkFName,
        const char *topFName, const char *beFName, const char *soFName,
        SchemaPtr schema) {
    ofstream out(outFName);

    // source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

//        MRNetFrontSourceOperatorConfig source(opID, "top_file", "backend", "filter.so", schema->getConfig());
        MRNetFrontSourceOperatorConfig source(opID, topFName , beFName , soFName, schema->getConfig());
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


int main(int argc, char** argv) {
    Flow_Init(0, NULL);

//    unsigned int numStreams= get_num_streams();
    unsigned int numStreams= NUM_STREAMS;

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFront();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getAggregate_Schema(numStreams);

    // Create a Flow and write it out to a configuration file.
//    createSource2SinkFlowFront(opConfigFName, "sink", "top_file", "backend", "filter.so", fileSchema);
    createSource2SinkFlowFront(CONFIG_FE, SINK_FILE, "top_file", "backend", "filter.so", fileSchema);

    // Load the flow we previously wrote to the configuration file and run it.
    FILE* opConfig = fopen(CONFIG_FE, "r");
    FILEStructureParser parser(opConfig, 10000);
    map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
    fclose(opConfig);

    // Show the data objects that got written to file "sink" by the flow
    assert(outSchemas.size()==1);
    cout << "Sink data:"<<endl;
    cout << "-------------------------------------------"<<endl;
    printDataFile("sink", outSchemas.begin()->second);
    cout << "-------------------------------------------"<<endl;

    Flow_Finalize();
    return 0;
}
