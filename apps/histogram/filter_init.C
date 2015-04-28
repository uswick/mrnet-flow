#include <unistd.h>
#include "filter_init.h"
#include "mrnet_operator.h"
#include "mrnet_flow.h"
#include "app_common.h"

//SharedPtr<SourceOperator> source_op;
SharedPtr<MRNetFilterOutOperator> out_op_filter;
map<unsigned int, SchemaPtr> output_schemas;


static void registerDeserializersFilter() {
    // Schemas
    SchemaRegistry::regCreator("Record", &RecordSchema::create);
    SchemaRegistry::regCreator("Scalar", &ScalarSchema::create);
    SchemaRegistry::regCreator("Histogram",  &HistogramSchema::create);
    SchemaRegistry::regCreator("HistogramBin", &HistogramBinSchema::create);

    // Operators
    OperatorRegistry::regCreator("MRNetFilterOut", &MRNetFilterOutOperator::create);
    OperatorRegistry::regCreator("SynchedRecordJoin", &SynchedRecordJoinOperator::create);
    OperatorRegistry::regCreator("MRNetFilterSource", &MRNetFilterSourceOperator::create);
}

// Test of Record and RecordSchema serialization/deserialization
SchemaPtr getInputSchemaFilterNode(int numFields) {
// The value will be a record
    RecordSchemaPtr recSchema = makePtr<RecordSchema>();
    for(int i = 0 ; i < numFields ; i++) {
        SharedPtr<ScalarSchema> recScalarSchema = makePtr<ScalarSchema>(ScalarSchema::doubleT);
        recSchema->add(txt() << "Rec_" << i,  recScalarSchema);
    }
    recSchema->finalize();
    return recSchema;
}

SharedPtr<MRNetFilterOutOperator>& getOutOperator(){
    return out_op_filter;
}

map<unsigned int, SchemaPtr>& getOutputSchemas(){
    return output_schemas;
}


void createFilterSource2Join2OutFlow(const char* outFName, SchemaPtr schema, unsigned int numStreams,
        double start, double stop, double bin_width) {
    ofstream out(outFName);

    // MRNet filter source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        MRNetFilterSourceOperatorConfig source(opID, numStreams, schema->getConfig());
        out << source.props->tagStr();
        ++opID;

        SynchedRecordJoinOperatorConfig join(/*numInputs*/ numStreams, opID, start, stop, bin_width);
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
    Flow_Init(0, NULL);
    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFilter();

    int numFileds = atoi(get_property(KEY_ITEMS_PER_RECORD).c_str());
    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getInputSchemaFilterNode(numFileds);

    // Create a Flow and write it out to a configuration file.
    double histogram_range_start, histogram_range_stop, histogram_col_width;

    //parse app.properties
    //default --> random numbers are generated between 10 - 150
    //hence lets 0 be start
    histogram_range_start = atof(get_property(KEY_HIST_START).c_str()) ;
    histogram_range_stop = atof(get_property(KEY_HIST_STOP).c_str()) ;
    //lets collect coulumns of 10 width
    histogram_col_width = atof(get_property(KEY_HIST_COL_WIDTH).c_str());
    
    
    printf("[Filter]: Application param initialization done. histogram_range_start : %f  histogram_range_stop : %f histogram_col_width : %f input_schema-numItems/Rec : %d child streams : %d\n", histogram_range_start, histogram_range_stop , histogram_col_width, numFileds, NUM_STREAMS);
    
    createFilterSource2Join2OutFlow(CONFIG_FILTER, fileSchema, NUM_STREAMS, histogram_range_start,
            histogram_range_stop, histogram_col_width);

    FILE* opConfig = fopen(CONFIG_FILTER, "r");
    FILEStructureParser parser(opConfig, 10000);

    glst_t filter_inf ;
    getFlowSource(parser, filter_inf);

    fprintf(stdout, "[Filter]: initialization complete now. PID : %d thread ID : %lu \n", getpid(), pthread_self());

    return filter_inf;
}
