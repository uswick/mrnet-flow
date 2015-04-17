#pragma once
#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"

using namespace std;

typedef struct {
    SharedPtr<SourceOperator> op;
    SharedPtr<Operator> sink;
    map<unsigned int, SchemaPtr> out_schemas;
} glst_t;

typedef enum{
    CP_FILTER=1110001, FE_BE_FILTER
} filter_type;


//#define ENABLE_HETRO_FILTERS

extern glst_t nullFilterInfo;

glst_t filter_flow_init();

#ifdef ENABLE_HETRO_FILTERS
glst_t hetro_filter_flow_init(filter_type type);
#endif