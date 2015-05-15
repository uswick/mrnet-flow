#pragma once

#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "filter_init.h"

typedef enum { FLOW_EXIT=1001, FLOW_START_PHASE,
    FLOW_START, FLOW_CLEANUP
} Protocol;

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


/*******************************************************
 * assign API for hetro filters
 * ***************************************************/

#ifdef ENABLE_HETRO_FILTERS
bool assign_filters( MRN::NetworkTopology* nettop, int be_filter, int cp_filter, int fe_filter, std::string& up );
#endif 
/***************************************
* Utility Functions for flow
***************************************/

int get_num_streams();

void printDataFile(const char* fName, SchemaPtr schema);

char* get_configBE();
char* get_configFE();
char* get_configFilter();

char* get_Source();
char* get_Sink();

#define CONFIG_FE get_configFE()
#define CONFIG_BE get_configBE()
#define CONFIG_FILTER get_configFilter()
#define SOURCE_FILE get_Source()
#define SINK_FILE get_Sink()


#define NUM_STREAMS get_num_streams()

extern const char* BE_filter ;
extern const char* FE_filter ;
extern const char* CP_filter ;

/***************************************
* Main Functions to execute flow engine
****************************************/
void Flow_Init(int argc, char** argv);

void Flow_Finalize();

std::map<unsigned int, SchemaPtr> runFlow(structureParser& parser);

SharedPtr<SourceOperator> getFlowSource(structureParser& parser, glst_t& filter_info);
//#define VERBOSE
