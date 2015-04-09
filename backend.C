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
    SchemaRegistry::regCreator("Tuple",  &TupleSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
    SchemaRegistry::regCreator("ExplicitKeyVal", &ExplicitKeyValSchema::create);

    // Operators
    OperatorRegistry::regCreator("InFile",  &InFileOperator::create);
    OperatorRegistry::regCreator("MRNetBackOut", &MRNetBEOutOperator::create);
}


// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getSchemaBackendNodeFillSource(const char *fName, unsigned int numStreams) {
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
    for(int i=0; i<1; ++i) {
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


void createSource2SinkFlowBackend(const char *outFName, const char *sourceFName, SchemaPtr schema) {
    ofstream out(outFName);

    // source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        InFileOperatorConfig source(opID, sourceFName, schema->getConfig());
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

    Flow_Init(argc, argv);
    //init params
    unsigned int numStreams= NUM_STREAMS;

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersBackend();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getSchemaBackendNodeFillSource(SOURCE_FILE, numStreams);

    // Create a BE MRNet Flow
    createSource2SinkFlowBackend(CONFIG_BE, SOURCE_FILE, fileSchema);

    // Load the flow we previously wrote to the configuration file and run it.
    FILE* opConfig = fopen(CONFIG_BE, "r");
    FILEStructureParser parser(opConfig, 10000);
    map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
    fclose(opConfig);
    
    Flow_Finalize();

    return 0;
}
