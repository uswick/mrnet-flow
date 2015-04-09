#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"
#include <iostream>
#include <stdio.h>
#include "app_common.h"

using namespace std;

//int __thread BE_ARG_CNT = 0 ;
//char**  BE_ARGS = NULL ;

// Registers deserializers for all the standard Schemas and Operators
static void registerDeserializersFront() {
    // Schemas
    SchemaRegistry::regCreator("Record", &RecordSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
    SchemaRegistry::regCreator("Histogram",  &HistogramSchema::create);
    SchemaRegistry::regCreator("HistogramBin", &HistogramBinSchema::create);

    // Operators
    OperatorRegistry::regCreator("MRNetFrontSource",  &MRNetFESourceOperator::create);
    OperatorRegistry::regCreator("SynchedHistogramJoin",  &SynchedHistogramJoinOperator::create);
    //create synch operator to aggregate Histograms
    OperatorRegistry::regCreator("OutFile", &OutFileOperator::create);
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

SchemaPtr getAggregate_Schema(){
    HistogramSchemaPtr outputHistogramSchema = makePtr<HistogramSchema>();
    return outputHistogramSchema;
}

void createSource2SinkFlowFront(const char *outFName, const char *sinkFName, int interval,
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

        SynchedHistogramJoinOperatorConfig histJoin(opID, interval);
        out << histJoin.props->tagStr();
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
        StreamConfig source2join(opID, 0, opID+1, 0);
        out << source2join.props.tagStr();
        ++opID;

        StreamConfig join2sink(opID, 0, opID+1, 0);
        out << join2sink.props.tagStr();

        out << streams.exitStr();
    }
}


int main(int argc, char** argv) {
//    const char* opConfigFName="opconfig_frontend";
//    if(argc>1) opConfigFName = argv[1];
    Flow_Init(0, NULL);

    //parse app.properties
    int sync_interval = atoi(get_property(KEY_SYNC_INTERVAL).c_str());
    
    cout << "[FE]: Application param initialization done. sync interval : " << sync_interval << endl ;

    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFront();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getAggregate_Schema();

    // Create a Flow and write it out to a configuration file.
    createSource2SinkFlowFront(CONFIG_FE, "sink", sync_interval , "top_file", "backend", "filter.so", fileSchema);

    // Load the flow we previously wrote to the configuration file and run it.
    FILE* opConfig = fopen(CONFIG_FE, "r");
    FILEStructureParser parser(opConfig, 10000);
    //clock_t strt = get_time();
    //t_pnt t1 = get_wall_time();
    map<unsigned int, SchemaPtr> outSchemas = runFlow(parser);
    //get_elapsed(strt, get_time(), t1, get_wall_time());
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
