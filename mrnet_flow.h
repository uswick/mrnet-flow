#pragma once

typedef enum { FLOW_EXIT=1001, FLOW_START_PHASE,
    FLOW_START, FLOW_CLEANUP
} Protocol;

int get_num_streams();

//#define VERBOSE
