#pragma once
#include "data.h"
#include "schema.h"
#include "operator.h"
#include "mrnet_operator.h"

using namespace std;

SharedPtr<SourceOperator>& filter_flow_init();

map<unsigned int, SchemaPtr>& getOutputSchemas();

SharedPtr<MRNetFilterOutOperator>& getOutOperator();