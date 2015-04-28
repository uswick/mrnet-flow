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
    OperatorRegistry::regCreator("SynchedHistogramJoin",  &SynchedHistogramJoinOperator::create);
    OperatorRegistry::regCreator("MRNetFilterSource", &MRNetFilterSourceOperator::create);
}

SchemaPtr getAggregate_Schema(){
    HistogramSchemaPtr outputHistogramSchema = makePtr<HistogramSchema>();
    return outputHistogramSchema;
}

SharedPtr<MRNetFilterOutOperator>& getOutOperator(){
    return out_op_filter;
}

map<unsigned int, SchemaPtr>& getOutputSchemas(){
    return output_schemas;
}


void createFilterSource2Join2OutFlow(const char* outFName, SchemaPtr schema, unsigned int numStreams,
        int interval) {
    ofstream out(outFName);

    // MRNet filter source -> sink

    {
        unsigned int opID=0;
        properties operators("Operators");
        out << operators.enterStr();

        MRNetFilterSourceOperatorConfig source(opID, numStreams, schema->getConfig());
        out << source.props->tagStr();
        ++opID;

        SynchedHistogramJoinOperatorConfig histJoin(opID, numStreams, interval);
        out << histJoin.props->tagStr();
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



glst_t flow_init(){
    Flow_Init(0, NULL);
    // First, register the deserializers for all the Schemas and Operators that may be used
    registerDeserializersFilter();

    // The flows we'll run get their input data from a file, so initialize the file to hold some data
    SchemaPtr fileSchema = getAggregate_Schema();

    // Create a Flow and write it out to a configuration file.
    int sync_interval = atoi(get_property(KEY_SYNC_INTERVAL).c_str());
    
    printf("[Filter]: Application param initialization done. sync interval : %d\n", sync_interval);
    
    //createFilterSource2Join2OutFlow(CONFIG_FILTER, fileSchema, NUM_STREAMS, sync_interval);
    createFilterSource2Join2OutFlow(CONFIG_FILTER, fileSchema, 2, sync_interval);

    FILE* opConfig = fopen(CONFIG_FILTER, "r");
    FILEStructureParser parser(opConfig, 10000);

    glst_t filter_inf ;
    getFlowSource(parser, filter_inf);

    fprintf(stdout, "[Filter]: initialization complete now. PID : %d thread ID : %lu \n", getpid(), pthread_self());

    return filter_inf;
}

#ifdef ENABLE_HETRO_FILTERS
glst_t hetro_filter_flow_init(filter_type type){
    #ifdef VERBOSE
    printf("[Filter]: Hetro Filter callback \n");
    #endif
    //in this case FE/BE and CP has all the same operators
    if(type == FE_BE_FILTER){
        return flow_init();
    }else if (type == CP_FILTER){
        return flow_init();
    }
}
#else
glst_t filter_flow_init(){
        return flow_init();
}
#endif
