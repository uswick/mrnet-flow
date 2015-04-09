#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>
#include "app_common.h"

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

// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getSchemaBackendNode(unsigned int numFields) {

    // The value will be a record
    RecordSchemaPtr recSchema = makePtr<RecordSchema>();
    for(int i = 0 ; i < numFields ; i++) {
        SharedPtr<ScalarSchema> recScalarSchema = makePtr<ScalarSchema>(ScalarSchema::doubleT);
        recSchema->add(txt() << "Rec_" << i,  recScalarSchema);
    }
    recSchema->finalize();
    return recSchema;
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

//int __thread BE_ARG_CNT = 0 ;
//char**  BE_ARGS = NULL ;

int main(int argc, char** argv) {
    #ifdef VERBOSE
    printf("[BE]: Starting backend with MRNet param initialization. arg count : %d  args : ", argc);
    for(int i = 0 ; i < argc ; i++){
        printf("[ %s ] ", argv[i]);
    }
    printf("\n");
    #endif
    //init params
//    BE_ARG_CNT = argc ;
//    BE_ARGS = argv ;
    Flow_Init(argc, argv);

    //parse app.properties
    int numFileds = atoi(get_property(KEY_ITEMS_PER_RECORD).c_str());
    int min = atoi(get_property(KEY_SOURCE_MIN).c_str());
    int max = atoi(get_property(KEY_SOURCE_MAX).c_str());
    int iters = atoi(get_property(KEY_SOURCE_ITERATIONS).c_str());

    printf("[BE]: Application param initialization done. numItems/Rec : %d  min_value : %d max_value : %d  genration iterations : %d \n", numFileds, min , max, iters);

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersBackend();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getSchemaBackendNode(numFileds);

    // Create a BE MRNet Flow
    createSource2SinkFlowBackend(CONFIG_BE, min, max, iters, fileSchema);

    // Load the flow we previously wrote to the configuration file and run it.
    FILE* opConfig = fopen(CONFIG_BE, "r");
    FILEStructureParser parser(opConfig, 10000);
    map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
    fclose(opConfig);

    Flow_Finalize();
    return 0;
}
