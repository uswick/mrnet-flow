#include "mrnet_operator.h"

using namespace MRN;


#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include "mrnet_flow.h"

using namespace std;

/***************************************
***** MRNetConfigs *****
***************************************/

/*****************************************
* MRNet Frontend config
*****************************************/

MRNetFrontSourceOperatorConfig::MRNetFrontSourceOperatorConfig(unsigned int ID, const char *topology_file,
        const char *backend_exec, const char *so_file, SchemaConfigPtr schemaCfg, propertiesPtr props) :
        OperatorConfig(/*numInputs*/ 0, /*numOutputs*/ 1, ID, setProperties(topology_file, backend_exec, so_file,
                schemaCfg, props)) {
}

propertiesPtr MRNetFrontSourceOperatorConfig::setProperties(const char *topology_file, const char *backend_exec,
        const char *so_file,
        SchemaConfigPtr schemaCfg, propertiesPtr props) {
    if (!props) props = boost::make_shared<properties>();

    map<string, string> pMap;
    pMap["topology_file"] = topology_file;
    pMap["backend_exec"] = backend_exec;
    pMap["so_file"] = so_file;

    props->add("MRNetFrontSource", pMap);

    // Add the properties of the schema as a sub-tag of props
    if (schemaCfg->props)
        props->addSubProp(schemaCfg->props);

    return props;
}

/*****************************************
* MRNet Filter Source config
*****************************************/

MRNetFilterSourceOperatorConfig::MRNetFilterSourceOperatorConfig(unsigned int ID, unsigned int num_outgoing_streams,
        SchemaConfigPtr schemaCfg,
        propertiesPtr props) :
        OperatorConfig(/*numInputs*/ 0, /*numOutputs*/ num_outgoing_streams, ID, setProperties(schemaCfg, props)) {
}

propertiesPtr MRNetFilterSourceOperatorConfig::setProperties(SchemaConfigPtr schemaCfg, propertiesPtr props) {
    if (!props) props = boost::make_shared<properties>();

    map<string, string> pMap;
    props->add("MRNetFilterSource", pMap);

    // Add the properties of the schema as a sub-tag of props
    if (schemaCfg->props)
        props->addSubProp(schemaCfg->props);

    return props;
}


/*****************************************
* MRNet Filter Sink config
*****************************************/

MRNetFilterOutOperatorConfig::MRNetFilterOutOperatorConfig(unsigned int ID, propertiesPtr props) :
        OperatorConfig(/*numInputs*/ 1, /*numOutputs*/ 0, ID, setProperties(props)) {
}

propertiesPtr MRNetFilterOutOperatorConfig::setProperties(propertiesPtr props) {
    if (!props) props = boost::make_shared<properties>();

    map<string, string> pMap;
    props->add("MRNetFilterOut", pMap);

    return props;
}

/*****************************************
* MRNet Backend Sink config
*****************************************/

MRNetBackendOutOperatorConfig::MRNetBackendOutOperatorConfig(unsigned int ID, propertiesPtr props) :
        OperatorConfig(/*numInputs*/ 1, /*numOutputs*/ 0, ID, setProperties(props)) {
}

propertiesPtr MRNetBackendOutOperatorConfig::setProperties(propertiesPtr props) {
    if (!props) props = boost::make_shared<properties>();

    map<string, string> pMap;
    props->add("MRNetBackOut", pMap);

    return props;
}

/***************************************
***** MRNet Operators *****
***************************************/

/***************************************
***** MRNetFilterSourceOperator *****
***************************************/

MRNetFilterSourceOperator::MRNetFilterSourceOperator(properties::iterator props) : SourceOperator(props.next()) {
    //init stream buffer
    char *internalBuffer = (char *) malloc(10000);
    streamBuf = new StreamBuffer(internalBuffer, 10000);

    assert(props.getContents().size() == 1);
    propertiesPtr schemaProps = *props.getContents().begin();
    schema = SchemaRegistry::create(schemaProps);
    assert(schema);
}


void MRNetFilterSourceOperator::setMRNetInfoObject(MRNetInfo &inf) {
    mrn_info = inf;
}

void MRNetFilterSourceOperator::work() {
#ifdef VERBOSE
    printf("[FilterSource]: start work().. PID : %d !! \n", getpid());
#endif
    unsigned length;
    bool be_node;
    for (unsigned int i = 0; i < mrn_info.packets_in->size(); i++) {
        PacketPtr curr_packet = (*mrn_info.packets_in)[i];

        Rank cur_inlet_rank = curr_packet->get_InletNodeRank();
        if (cur_inlet_rank == -1) {
            be_node = true;
        }
        //handle special case - BE sync and case where node is down
        if (cur_inlet_rank != UNKNOWN_NODE && mrn_info.packets_in->size() != 1) {
            if (mrn_info.net->node_Failed(cur_inlet_rank)) {
#ifdef VERBOSE
                printf("[NODE FAILED ] \n");
#endif
                // drop packets from failed node
                continue;
            }
        }
        //first assert if protocol exit tag is indicated
        int tag_id = curr_packet->get_Tag();
        if(tag_id == FLOW_EXIT){
            #ifdef VERBOSE
            printf("[FilterSource]: recieved EXIT token via inlet rank : %u, forwarding to outgoing streams.. PID : %d !! \n"
                    ,(unsigned int) cur_inlet_rank, getpid());
            #endif
            outStreams[(unsigned int) cur_inlet_rank]->streamFinished();
            continue;
        }

        #ifdef VERBOSE
        printf("[FilterSource]: unpacking/deserialization in progress.. PID : %d !! \n", getpid());
        #endif

        char *recv_Ar;
        curr_packet->unpack("%ac", &recv_Ar, &length);

        //deserialize stream data using using a incoming stream Buffer and inject to outgoing FLOW
        //filters at front end would generally produce a merged stream using MRNet filters
        assert((unsigned int) cur_inlet_rank < outStreams.size());
#ifdef VERBOSE
        fprintf(stdout, "[FilterSource]: before buffer stream write() : buffer total: %d  buffer max: %d  buffer start : %d buffer seek : %d ..\n",
                streamBuf->current_total_size, streamBuf->max_size, streamBuf->start, streamBuf->seek);
#endif

        Schema::bufwrite(recv_Ar, length, streamBuf);

#ifdef VERBOSE
        fprintf(stdout, "[FilterSource]: Starting to recv serial data  buffer total: %d  buffer max: %d  buffer start : %d buffer seek : %d ..\n",
                streamBuf->current_total_size, streamBuf->max_size, streamBuf->start, streamBuf->seek);
        int j = 0;
        for (j = 0; j < streamBuf->current_total_size; j++) {
            printf("%c", *((char *) streamBuf->buffer + ((streamBuf->start + j) % streamBuf->max_size)));
        }

        printf("\n[FilterSource]:---------------- \n\n\n");
        printf("[FilterSource]: print schema --> \n");
        schema->str(cout);
#endif

        DataPtr data = schema->deserialize(streamBuf);

#ifdef VERBOSE
        if (data == NULLData) {
            printf("[FilterSource]: deserialization routine returned NULL data .. \n");
        } else {
            printf("[FilterSource]: data deserialization sucessfull from rank : %d.. \n", cur_inlet_rank);
            printf("[FilterSource]: data ready for outflow.. \n");
            data->str(cout, schema);
            printf("\n---------------- \n\n");
        }
#endif
        //route data ptr to respective outgoing stream
        if (data != NULLData)
            outStreams[(unsigned int) cur_inlet_rank]->transfer(data);

        //remove space taken by buffer
        delete recv_Ar;
    }
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetFilterSourceOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    int i = 0;
    for (i = 0; i < numOutputs; ++i) {
        schemas.push_back(schema);
    }
    return schemas;
}

// Called to signal that all the outgoing streams have been connected.
// Inside this call the operator may send Data objects on the outgoing streams.
// After this call the operator's work() function may be called.
void MRNetFilterSourceOperator::outConnectionsComplete() {
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream &MRNetFilterSourceOperator::str(std::ostream &out) const {
    out << "[MRNFilterSourceOperator: ";
    Operator::str(out);
    out << "]";
    return out;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr MRNetFilterSourceOperator::create(properties::iterator props) {
    assert(props.name() == "MRNetFilterSource");
    return makePtr<MRNetFilterSourceOperator>(props);
}

MRNetFilterSourceOperator::~MRNetFilterSourceOperator() {
    delete streamBuf->buffer;
    delete streamBuf;
}

/*********************************
***** MRNetFilterOutOperator *****
*********************************/

MRNetFilterOutOperator::MRNetFilterOutOperator(properties::iterator props) : AsynchOperator(props.next()) {

}

void MRNetFilterOutOperator::setMRNetInfoObject(MRNetInfo &inf) {
    mrn_info = inf;
    packets_out = inf.packets_out;
}

//indicates when incoming stream has finised its operation
//ie:- KeyValJoin operator has recieved all exit tokens
void MRNetFilterOutOperator::inStreamFinished(unsigned int inStreamIdx){
    #ifdef VERBOSE
     printf("[FilterOut]: preparing [Final] packet  PID : %d !! \n", getpid());
    #endif
    Packet *pckt = new Packet(mrn_info.stream_id, FLOW_EXIT, "%d", 0);
    PacketPtr new_packet(pckt);
//    assert(packets_out)
    //todo verify we have a valid output packet vector
    packets_out->push_back(new_packet);
}

void MRNetFilterOutOperator::work(unsigned int inStreamIdx, DataPtr inData) {
#ifdef VERBOSE
    printf("[FilterOut]: start work().. PID : %d !! \n", getpid());
#endif
    bool final_packet = false;
    Packet *pckt;

    assert(inStreamIdx == 0);
    char *out_buffer = (char *) malloc(10000);
//    char out_buffer[10000];

    //create stream buffer
    StreamBuffer bufferStream(out_buffer, 10000);

    //create serialized stream on buffer using schema and data obj
    inStreams[0]->getSchema()->serialize(inData, &bufferStream);

    //todo determine final packet
    #ifdef VERBOSE
    fprintf(stdout, "[FilterOut]: printing data to be sent upstream.. total bytes : %d \n", bufferStream.current_total_size);
    int j = 0;
    for (j = 0; j < bufferStream.current_total_size; j++) {
        printf("%c", out_buffer[j]);
    }
    printf("\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ \n\n\n");
    #endif

    if (!final_packet) {
        #ifdef VERBOSE
        printf("[FilterOut]: preparing packet  PID : %d !! \n", getpid());
        #endif
        pckt = new Packet(mrn_info.stream_id, mrn_info.tag_id, "%auc", out_buffer,
                bufferStream.current_total_size);
    } else {
        #ifdef VERBOSE
        printf("[FilterOut]: preparing [Final] packet  PID : %d !! \n", getpid());
        #endif
        pckt = new Packet(mrn_info.stream_id, FLOW_EXIT, "%auc", out_buffer,
                bufferStream.current_total_size);
    }

    #ifdef VERBOSE
    printf("[FilterOut]: print schema --> \n");
    inStreams[0]->getSchema()->str(cout);
    #endif


// dummy test deserializtion
//    Schema::bufwrite(tmp_buffer, buf.current_total_size, &buf);
//    Schema::bufwrite(out_buffer, bufferStream.current_total_size, &buf);
//    DataPtr data = inStreams[0]->getSchema()->deserialize(&buf);

    pckt->set_DestroyData(true) ;
    PacketPtr new_packet(pckt);
    packets_out->push_back(new_packet);
    #ifdef VERBOSE
    printf("[FilterOut]: pushed back data upstream..  PID : %d !! \n", getpid());
    #endif

}


// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetFilterOutOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    return schemas;
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream &MRNetFilterOutOperator::str(std::ostream &out) const {
    out << "[MRNFilterOutOperator: ";
    Operator::str(out);
    out << "]";
    return out;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr MRNetFilterOutOperator::create(properties::iterator props) {
    assert(props.name() == "MRNetFilterOut");
    return makePtr<MRNetFilterOutOperator>(props);
}

MRNetFilterOutOperator::~MRNetFilterOutOperator() {
}

/***************************************
***** MRNetFESourceOperator **************
***************************************/
bool saw_failure;

void MRNetFESourceOperator::Failure_Callback(Event *evt, void *) {
    if ((evt->get_Class() == Event::TOPOLOGY_EVENT) &&
            (evt->get_Type() == TopologyEvent::TOPOL_REMOVE_NODE))
        saw_failure = true;
}

MRNetFESourceOperator::MRNetFESourceOperator(properties::iterator props) : SourceOperator(props.next()) {
    topology_file = (char *) props.get("topology_file").c_str();
    assert(topology_file);

    backend_exe = (char *) props.get("backend_exec").c_str();
    assert(backend_exe);

    so_file = (char *) props.get("so_file").c_str();
    assert(so_file);

    dummy_argv = NULL;
    assert(props.getContents().size() == 1);
    propertiesPtr schemaProps = *props.getContents().begin();
    schema = SchemaRegistry::create(schemaProps);
    assert(schema);

    //init stream buffer
    char *internalBuffer = (char *) malloc(10000);
    streamBuf = new StreamBuffer(internalBuffer, 10000);

    int ret = initMRNet();
    assert(ret);
    init = false;
    fprintf(stdout, "[FE]: initialization complete PID : %d thread ID : %lu \n", getpid(), pthread_self());
}

int MRNetFESourceOperator::initMRNet() {
    net = Network::CreateNetworkFE(topology_file, backend_exe, &dummy_argv);
    if (net->has_Error()) {
        net->perror("[FE]: Network creation failed");
        exit(-1);
    }

    if (!net->set_FailureRecovery(false)) {
        fprintf(stdout, "[FE]: Failed to disable failure recovery\n");
        delete net;
        return -1;
    }
    bool cbrett = net->register_EventCallback(Event::TOPOLOGY_EVENT,
            TopologyEvent::TOPOL_REMOVE_NODE,
            MRNetFESourceOperator::Failure_Callback, NULL);
    if (cbrett == false) {
        fprintf(stdout, "[FE]: Failed to register callback for node failure topology event\n");
        delete net;
        return -1;
    }

    // Make sure path to "so_file" is in LD_LIBRARY_PATH
    int filter_id = net->load_FilterFunc(so_file, "defaultFlowFilter");
    if (filter_id == -1) {
        fprintf(stderr, "[FE]: Network::load_FilterFunc() failure\n");
        delete net;
        return -1;
    }
    #ifdef VERBOSE
    printf("[FE]: loading mrnet filter done !! \n");
    #endif

    // A Broadcast communicator contains all the back-ends
    comm_BC = net->get_BroadcastCommunicator();

    // Create a stream that will use the Integer_Add filter for aggregation
    active_stream = net->new_Stream(comm_BC, filter_id,
            SFILTER_WAITFORALL);

    num_backends = int(comm_BC->get_EndPoints().size());
    return 1;
}

void MRNetFESourceOperator::work() {
    int tag;
    PacketPtr p;
    if (!init) {
        int send_val = 32;
        // Broadcast a control message to back-ends to send us "num_iters"
        // waves of integers
        tag = FLOW_START_PHASE;
        if (active_stream->send(tag, "%d ", send_val) == -1) {
            fprintf(stderr, "[FE]: stream::send() failure\n");
            return;
        }
        if (active_stream->flush() == -1) {
            fprintf(stderr, "[FE]: stream::flush() failure\n");
            return;
        }
        init = true;
    }

    while (true) {
        #ifdef VERBOSE
        printf("[FE]: init done... waiting for packet.. PID : %d thread ID: %lu \n", getpid(), pthread_self());
        fflush(stdout);
        #endif

        int retval = active_stream->recv(&tag, p);

        #ifdef VERBOSE
        printf("[FE]: MRNet packet recieved succesfully ..\n");
        fflush(stdout);
        #endif
        //check errors in incoming communication
        if (retval == 0) {
            //shouldn't be 0, either error or block for data, unless a failure occured
            fprintf(stderr, "[FE]: stream::recv() returned zero\n");
            if (saw_failure) break;
        }
        if (retval == -1) {
            //recv error
            fprintf(stderr, "[FE]: stream::recv() unexpected failure\n");
            if (saw_failure) break;
            return;
        }

        //if this is the Exit phase break from loop
        if (tag == FLOW_EXIT) {
            printf("[FE]: Recieved EXIT tag from MRNet Stream, moving into final phase..\n");
            outStreams[0]->streamFinished();
            break;
        }
        //not the exit phase
        const char *recv_Ar;
        int length;

        if (p->unpack("%auc", &recv_Ar, &length) == -1) {
            fprintf(stderr, "[FE]: PROTOCOL END stream::unpack() failure\n");
            return;
        }

        #ifdef VERBOSE
        fprintf(stdout, "[FE]: Starting to recv serial data (unsigned char array) from children  ==> total bytes : %d \n", length);
        int j = 0;
        for (j = 0; j < length; j++) {
            printf("%c", recv_Ar[j]);
        }
        printf("\n[FE]: ---------------- \n\n\n");
        //deserialize stream data using using a incoming stream Buffer and inject to outgoing FLOW
        //filters at front end would generally produce a merged stream using MRNet filter#ifdef VERBOSE

        printf("[FE]: print schema --> \n");
        schema->str(cout);
        #endif

//        char tmp_buffer[10000];
//        StreamBuffer buf(tmp_buffer, 10000);

        //deserialize data from rececieved information
//        Schema::bufwrite(recv_Ar, length, &buf);
        Schema::bufwrite(recv_Ar, length, streamBuf);
        DataPtr data = schema->deserialize(streamBuf);
#ifdef VERBOSE
        data->str(cout, schema);
#endif

        if (data != NULLData) {
            #ifdef VERBOSE
            printf("[FE]: data deserialization sucessfull. ready for sink...PID : %d \n", getpid());
            #endif
            outStreams[0]->transfer(data);
        } else {
            #ifdef VERBOSE
            printf("[FE]: data deserialization failed...PID : %d \n", getpid());
            #endif
        }

        //remove space taken by buffer
        delete recv_Ar;
    }
#ifdef VERBOSE
    printf("[FE]: [WARN !!] exited main communication loop.. PID : %d \n", getpid());
#endif

    if (saw_failure) {
        fprintf(stderr, "[FE]: a network process has failed, killing network\n");
        delete net;
    }
    else {
        delete active_stream;
        delete net;
        printf("[FE]: Flow data transfer is successfull... !! PID : %d \n\n", getpid());
        // Tell back-ends to exit
//        MRN::Stream *ctl_stream = net->new_Stream(comm_BC, TFILTER_MAX,
//                SFILTER_WAITFORALL);
//        if (ctl_stream->send(FLOW_CLEANUP, "") == -1) {
//            fprintf(stderr, "[FE]: stream::send(exit) failure\n");
//            return;
//        }
//        if (ctl_stream->flush() == -1) {
//            fprintf(stderr, "[FE]: stream::flush() failure\n");
//            return;
//        }
//        int retval = ctl_stream->recv(&tag, p);
//        if (retval == -1) {
//            //recv error
//            fprintf(stderr, "[FE]: stream::recv() failure\n");
//            return;
//        }
//        delete ctl_stream;
//        if (tag == FLOW_CLEANUP) {
//            // The Network destructor will cause all internal and leaf tree nodes to exit
//            delete net;
//        }
    }


}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetFESourceOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    int i = 0;
    for (i = 0; i < numOutputs; ++i) {
        schemas.push_back(schema);
    }
    return schemas;
}

// Called to signal that all the outgoing streams have been connected.
// Inside this call the operator may send Data objects on the outgoing streams.
// After this call the operator's work() function may be called.
void MRNetFESourceOperator::outConnectionsComplete() {
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream &MRNetFESourceOperator::str(std::ostream &out) const {
    out << "[MRNFrontSourceOperator: ";
    Operator::str(out);
    out << "]";
    return out;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr MRNetFESourceOperator::create(properties::iterator props) {
    assert(props.name() == "MRNetFrontSource");
    return makePtr<MRNetFESourceOperator>(props);
}

MRNetFESourceOperator::~MRNetFESourceOperator() {
    delete streamBuf->buffer;
    delete streamBuf;
}

/**********************************************
***** MRNetBEOutOperator **************
**********************************************/
extern int __thread
BE_ARG_CNT;
extern char **BE_ARGS;

using namespace MRN;

MRNetBEOutOperator::MRNetBEOutOperator(properties::iterator props) : AsynchOperator(props.next()) {
    net = Network::CreateNetworkBE(BE_ARG_CNT, BE_ARGS);
    init = false;
    fprintf(stdout, "[BE]: initialization complete PID : %d thread ID : %lu  \n", getpid(), pthread_self());
}

void MRNetBEOutOperator::inStreamFinished(unsigned int inStreamIdx) {
    //only one stream is present in backend
    //send exit tag immediately
    int tag = FLOW_EXIT;
    if (stream->send(tag, "%d", 0) == -1) {
        fprintf(stderr, "[BE]: stream::send(%%s) failure in FLOW_EXIT\n");
    }
    if (stream->flush() == -1) {
        fprintf(stderr, "[BE]: stream::flush() failure in FLOW_EXIT\n");
    }

    ///wait untill stream is closed
    if( stream != NULL ) {
        while( ! stream->is_Closed() )
            sleep(1);

        delete stream;
    }

    // FE delete of the net will cause us to exit, wait for it
    net->waitfor_ShutDown();
    delete net;
}

void MRNetBEOutOperator::work(unsigned int inStreamIdx, DataPtr inData) {

    PacketPtr p;
    int rc, tag = 0, recv_val = 0;
    {

        //do this at start - get the tag from FE for initial communication
        if (!init) {
            rc = net->recv(&tag, p, &stream);
            #ifdef VERBOSE
            printf("BE: starting initial communication phase \n");
            #endif
            if (rc == -1) {
                fprintf(stderr, "[BE]: Network::recv() failure\n");
                tag = FLOW_EXIT;
            }
            else if (rc == 0) {
                // a stream was closed
                tag = FLOW_EXIT;
            }
            tag = FLOW_START_PHASE;
        } else {
            //if this is not the init phase we always assume a FLOW is active
            tag = FLOW_START_PHASE;
        }

        switch (tag) {

            case FLOW_START_PHASE: {
                if (!init) {
                    p->unpack("%d ", &recv_val);
                    assert(recv_val > 0);
                    #ifdef VERBOSE
                    printf("[BE]: recieved initial token value : %d \n", recv_val);
                    #endif
                    //finished init phase
                    init = true;
                }

                // then drive flow from a file source -> MRNet sink
                //serialize Dataptr object upstream as and when #work is invoked
                #ifdef VERBOSE
                fprintf(stdout, "[BE]: Starting to Send wave of data upstream..\n");
                #endif
                assert(inStreamIdx == 0);
                char *out_buffer = (char *) malloc(1000);

                #ifdef VERBOSE
                fprintf(stdout, "[BE]: Init buffer writers..\n");
                #endif
                //create stream buffer
                StreamBuffer bufferStream(out_buffer, 1000);
                //create serialized stream on buffer using schema and data obj
                inStreams[0]->getSchema()->serialize(inData, &bufferStream);

                #ifdef VERBOSE
                fprintf(stdout, "[BE]: send() call being initiated..\n");
                int j = 0;
                for (j = 0; j < bufferStream.current_total_size; j++) {
                    printf("%c", out_buffer[j]);
                }
                printf("\n[BE]: ---------------- \n\n\n");
                #endif

                if (stream->send(tag, "%ac", out_buffer, bufferStream.current_total_size) == -1) {
//                if (stream->send(tag, "%ac", tmp, 10) == -1) {
                    fprintf(stderr, "[BE]: stream::send(%%d) failure in FLOW_START_PHASE\n");
                    tag = FLOW_EXIT;
                    break;
                }
                if (stream->flush() == -1) {
                    fprintf(stderr, "[BE]: stream::flush() failure in FLOW_START_PHASE\n");
                    break;
                }
//                delete out_buffer;
                fflush(stdout);
                sleep(1); // stagger sends
            }
                break;

            case FLOW_EXIT: {
                fprintf(stdout, "[BE]: switch case FLOW_EXIT call being initiated..PID : %d \n", getpid());
                //terminate flow
                inStreamFinished(0);
            }
                break;

            default: {
                fprintf(stdout, "[BE]: switch case default call being initiated..PID : %d \n", getpid());
                fprintf(stderr, "[BE]: Unknown Protocol: %d\n", tag);
                tag = FLOW_EXIT;
                inStreamFinished(0);
            }
                break;
        }

        fflush(stderr);

    }


}

MRNetBEOutOperator::~MRNetBEOutOperator() {
}


// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetBEOutOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    return schemas;
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream &MRNetBEOutOperator::str(std::ostream &out) const {
    out << "[MRNBackEndOutOperator: ";
    Operator::str(out);
    out << "]";
    return out;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr MRNetBEOutOperator::create(properties::iterator props) {
    assert(props.name() == "MRNetBackOut");
    return makePtr<MRNetBEOutOperator>(props);
}
