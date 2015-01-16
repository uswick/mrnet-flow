/****************************************************************************
 * Copyright � Udayanga Wickramasinghe - Indiana University                 *
 *                                                                          *
 ****************************************************************************/

#include <vector>

#include "mrnet/Packet.h"
#include "mrnet/NetworkTopology.h"
#include "mrnet_flow.h"
#include "operator.h"
#include "mrnet_operator.h"
#include "filter_init.h"

#include <string.h>

using namespace MRN ;
/**
* each filter initialize a Flow
* minimally each flow has a
*   MRNet source --> (operator) --> MRNet Sink
*
*   This acts as the driver for 'Flow' in comm/front/be nodes
*   source->driver() invoked each time a packet/s is delevered to this filter
*/
extern "C" {

/*
* Let filter accept characters arrays
* */
const char *defaultFlowFilter_format_string = "%ac";

typedef struct {
    SharedPtr<SourceOperator> op;
    SharedPtr<MRNetFilterOutOperator> sink;
} glst_t;


glst_t *initAndGetGlobal(void **, MRNetInfo& minfo);

bool filter_initialized = false ;
/**
* This is the entry point of a MRNet filter - each time packets are routed up the MRNet tree each node
* invokes a specific filter registerd at the front end. In this case  'defaultFlowFilter' is what
* solely resposible for stream filtering at runtime.
*/
void defaultFlowFilter(std::vector< PacketPtr > &packets_in,
        std::vector< PacketPtr > &packets_out,
        std::vector< PacketPtr > & /* packets_out_reverse */,
        void **state_data,
        PacketPtr & /* params */,
        const TopologyLocalInfo &inf) {

//#ifdef DEBUG_ON
    fprintf(stdout, "[MRNet FILTER METHOD just started.. PID : %d ]\n", getpid());
    fflush(stdout);
//#endif
    Network *net = const_cast< Network * >( inf.get_Network() );
    PacketPtr first_packet = packets_in[0];
    int stream_id = first_packet->get_StreamId();
    int tag_id = first_packet->get_Tag();
    MRN::Stream *stream = net->get_Stream(stream_id);
    std::set< Rank > peers;
    stream->get_ChildRanks(peers);
    //handle special BE case
    if (peers.size() == 0 && first_packet->get_InletNodeRank() == -1) {
        Rank r = -1;
        peers.insert(-1);
        //special BE optimization
        std::vector< PacketPtr >::iterator in;
        for( in = packets_in.begin() ; in != packets_in.end(); in++) {
            packets_out.push_back(*in);
        }
        return;
    }
    //create parameter object
    MRNetInfo minfo;
    minfo.net = net;
    minfo.packets_in = &packets_in;
    minfo.peers = peers ;
    minfo.stream = stream;
    minfo.stream_id =stream_id;
    minfo.tag_id = tag_id;

    glst_t *state = initAndGetGlobal(state_data, minfo);

    //initialize with MRNet runtime data
    SharedPtr<MRNetFilterSourceOperator> source_op = dynamicPtrCast<MRNetFilterSourceOperator>(state->op);
    source_op->setMRNetInfoObject(minfo);
    state->sink->setMRNetInfoObject(minfo);

    //exectue workflow
    state->op->work();
}

/**
* Initialize/setup state - state_data will persist through out the stream communication
* init communication constructs including queues/signals and threads.
*/
glst_t *initAndGetGlobal(void **state_data, MRNetInfo& minfo) {
    glst_t *global_state;
    if (*state_data == NULL && !filter_initialized) {
        filter_initialized = true ;
        global_state = new glst_t;
        SharedPtr<SourceOperator> source = filter_flow_init();
        global_state->op = source;

        global_state->sink = getOutOperator();
        //source - mrnet filter source

        *state_data = global_state;
    } else {
        global_state = (glst_t *) (*state_data);
    }

    //set parameter object


#ifdef DEBUG_ON
    fprintf(stdout, "MRNet FILTER method - Init Phase completed PID : %d \n", getpid());
#endif
    return global_state;
}


} /* extern "C" */
