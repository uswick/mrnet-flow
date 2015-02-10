#pragma once
#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"

using namespace std;

typedef struct {
    SharedPtr<SourceOperator> op;
    SharedPtr<MRNetFilterOutOperator> sink;
    map<unsigned int, SchemaPtr> out_schemas;
} glst_t;

glst_t filter_flow_init();
