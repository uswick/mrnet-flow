#include "mrnet_operator.h"
using namespace MRN ;


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

    assert(props.getContents().size()==1);
    propertiesPtr schemaProps = *props.getContents().begin();
    schema = SchemaRegistry::create(schemaProps);
    assert(schema);
}


void MRNetFilterSourceOperator::setMRNetInfoObject(MRNetInfo &inf) {
    mrn_info = inf;
}

void MRNetFilterSourceOperator::work() {
    printf("mrnet filter source operator.... PID : %d !! \n", getpid());
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
#ifdef DEBUG_ON
                printf("[NODE FAILED ] \n");
#endif
                // drop packets from failed node
                continue;
            }
        }
        printf("mrnet filter source operator unpacking/deserialization on progress.. PID : %d !! \n", getpid());

        char *recv_Ar;
        curr_packet->unpack("%ac", &recv_Ar, &length);

        //deserialize stream data using using a incoming stream Buffer and inject to outgoing FLOW
        //filters at front end would generally produce a merged stream using MRNet filters
        assert((unsigned int) cur_inlet_rank < outStreams.size());

        Schema::bufwrite(recv_Ar, length, streamBuf);
        fprintf(stdout, "FILTER: Starting to recv serial data..\n");
        int j = 0 ;
        for( j = 0 ; j < streamBuf->current_total_size ; j++){
            printf("%c",((char*)streamBuf->buffer)[j]);
        }
        printf("\n---------------- \n\n\n");
        printf("print schema --> \n");
        schema->str(cout);
        printf("print schema done.. --> \n");
        DataPtr data = schema->deserialize(streamBuf);

        if(data == NULLData){
            printf("returned NULL data .. \n");
        }else{
            printf("returned data OK current rank : %d ..printing ==> \n", cur_inlet_rank);
            data->str(cout, schema);
            printf("\n---------------- \n\n");
        }
        //route data ptr to respective outgoing stream
        if (data != NULLData)
            outStreams[(unsigned int) cur_inlet_rank]->transfer(data);


    }
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetFilterSourceOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    schemas.push_back(schema);
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

MRNetFilterSourceOperator::~MRNetFilterSourceOperator(){

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

void MRNetFilterOutOperator::work(unsigned int inStreamIdx, DataPtr inData) {
    printf("mrnet filter out operator started.... PID : %d !! \n", getpid());
    bool final_packet = false;
    Packet *pckt;

    assert(inStreamIdx == 0);
    char *out_buffer = (char *) malloc(10000);

    //create stream buffer
    StreamBuffer bufferStream(out_buffer, 10000);

    //create serialized stream on buffer using schema and data obj
    inStreams[0]->getSchema()->serialize(inData, &bufferStream);

    //todo determine final packet
    fprintf(stdout, "FILTER - OUT: Starting to send data.. total bytes : %d \n", bufferStream.current_total_size);
    int j = 0 ;
    for( j = 0 ; j < bufferStream.current_total_size ; j++){
        printf("%c",((char*)bufferStream.buffer)[j]);
    }
    printf("\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ \n\n\n");


    if (!final_packet) {
        printf("mrnet filter out operator preparing packet  PID : %d !! \n", getpid());
        pckt = new Packet(mrn_info.stream_id, mrn_info.tag_id, "%ac", bufferStream.buffer,
                bufferStream.current_total_size);
    } else {
        printf("mrnet filter out operator preparing [Final] packet  PID : %d !! \n", getpid());
        pckt = new Packet(mrn_info.stream_id, FLOW_EXIT, "%ac", bufferStream.buffer,
                bufferStream.current_total_size);
    }
    PacketPtr new_packet(pckt);
    packets_out->push_back(new_packet);
    printf("mrnet filter out operator push back upstream  PID : %d !! \n", getpid());

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

MRNetFilterOutOperator::~MRNetFilterOutOperator(){
}
/***************************************
***** MRNetFESourceOperator **************
***************************************/
bool saw_failure   ;

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

    //init stream buffer
    char *internalBuffer = (char *) malloc(10000);
    streamBuf = new StreamBuffer(internalBuffer, 10000);

    assert(props.getContents().size() == 1);
    propertiesPtr schemaProps = *props.getContents().begin();
    schema = SchemaRegistry::create(schemaProps);
    assert(schema);

    int ret = initMRNet();
    assert(ret);
    init = false;

}

int MRNetFESourceOperator::initMRNet() {
    net = Network::CreateNetworkFE(topology_file, backend_exe, &dummy_argv);
    if (net->has_Error()) {
        net->perror("Network creation failed");
        exit(-1);
    }

    if (!net->set_FailureRecovery(false)) {
        fprintf(stdout, "Failed to disable failure recovery\n");
        delete net;
        return -1;
    }
    bool cbrett = net->register_EventCallback(Event::TOPOLOGY_EVENT,
            TopologyEvent::TOPOL_REMOVE_NODE,
            MRNetFESourceOperator::Failure_Callback, NULL);
    if (cbrett == false) {
        fprintf(stdout, "Failed to register callback for node failure topology event\n");
        delete net;
        return -1;
    }

    // Make sure path to "so_file" is in LD_LIBRARY_PATH
    int filter_id = net->load_FilterFunc(so_file, "defaultFlowFilter");
    if (filter_id == -1) {
        fprintf(stderr, "Network::load_FilterFunc() failure\n");
        delete net;
        return -1;
    }
    printf("loading mrnet filter done !! \n");
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
            fprintf(stderr, "stream::send() failure\n");
            return;
        }
        if (active_stream->flush() == -1) {
            fprintf(stderr, "stream::flush() failure\n");
            return;
        }
        init = true;
    }

    while (true) {
        printf("FE init done... waiting for packet ..\n");
        int retval = active_stream->recv(&tag, p);
        printf("FE waiting for packet done ..\n");

        //check errors in incoming communication
        if (retval == 0) {
            //shouldn't be 0, either error or block for data, unless a failure occured
            fprintf(stderr, "stream::recv() returned zero\n");
            if (saw_failure) break;
        }
        if (retval == -1) {
            //recv error
            fprintf(stderr, "stream::recv() unexpected failure\n");
            if (saw_failure) break;
            return;
        }

        //if this is the Exit phase break from loop
        if (tag == FLOW_EXIT) {
            break;

        }
        //not the exit phase
        char *recv_Ar;
        int length;
        if (p->unpack("%ac", &recv_Ar, &length) == -1) {
            fprintf(stderr, "PROTOCOL END stream::unpack() failure\n");
            return;
        }

        fprintf(stdout, "FE: Starting to recv serial data from children  ==> total bytes : %d \n", length);
        int j = 0 ;
        for( j = 0 ; j < length ; j++){
            printf("%c",recv_Ar[j]);
        }
        printf("\n---------------- \n\n\n");

        //deserialize stream data using using a incoming stream Buffer and inject to outgoing FLOW
        //filters at front end would generally produce a merged stream using MRNet filters
        //TODO
        Schema::bufwrite(recv_Ar, length, streamBuf);
        DataPtr data = schema->deserialize(streamBuf);

        if (data != NULLData)
            outStreams[0]->transfer(data);

    }

    if (saw_failure) {
        fprintf(stderr, "FE: a network process has failed, killing network\n");
        delete net;
    }
    else {
        delete active_stream;

        // Tell back-ends to exit
        MRN::Stream *ctl_stream = net->new_Stream(comm_BC, TFILTER_MAX,
                SFILTER_WAITFORALL);
        if (ctl_stream->send(FLOW_EXIT, "") == -1) {
            fprintf(stderr, "stream::send(exit) failure\n");
            return;
        }
        if (ctl_stream->flush() == -1) {
            fprintf(stderr, "stream::flush() failure\n");
            return;
        }
        int retval = ctl_stream->recv(&tag, p);
        if (retval == -1) {
            //recv error
            fprintf(stderr, "stream::recv() failure\n");
            return;
        }
        delete ctl_stream;
        if (tag == FLOW_EXIT) {
            // The Network destructor will cause all internal and leaf tree nodes to exit
            delete net;
        }
    }


}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> MRNetFESourceOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    schemas.push_back(schema);
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
}

/**********************************************
***** MRNetBEOutOperator **************
**********************************************/
extern int BE_ARG_CNT ;
extern char** BE_ARGS;

MRNetBEOutOperator::MRNetBEOutOperator(properties::iterator props) : AsynchOperator(props.next()) {
    net = Network::CreateNetworkBE(BE_ARG_CNT, BE_ARGS);
    init = false;
}

void MRNetBEOutOperator::work(unsigned int inStreamIdx, DataPtr inData) {

    PacketPtr p;
    int rc, tag = 0, recv_val = 0;
    {

        //do this at start - get the tag from FE for initial communication
        if (!init) {
            rc = net->recv(&tag, p, &stream);
            printf("BE: recieved initial token \n");
            if (rc == -1) {
                fprintf(stderr, "BE: Network::recv() failure\n");
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
                    printf("BE: recieved token value : %d \n", recv_val);
                    //finished init phase
                    init = true;
                }

                // then drive flow from a file source -> MRNet sink
                //serialize Dataptr object upstream as and when #work is invoked
                fprintf(stdout, "BE: Sending wave..\n");
                assert(inStreamIdx == 0);
                char *out_buffer = (char *) malloc(1000);

                fprintf(stdout, "BE: Init buffer..\n");
                //create stream buffer
                StreamBuffer bufferStream(out_buffer, 1000);

                fprintf(stdout, "BE: Init Stream..\n");
                //create serialized stream on buffer using schema and data obj
                inStreams[0]->getSchema()->serialize(inData, &bufferStream);
                fprintf(stdout, "BE: Starting to send serialized data..\n");
                int j = 0 ;
                for( j = 0 ; j < bufferStream.current_total_size ; j++){
                    printf("%c",out_buffer[j]);
                }
                printf("\n---------------- \n\n\n");
                //test
//                char tmp[10] = "abcdef123";

                if (stream->send(tag, "%ac", out_buffer, bufferStream.current_total_size) == -1) {
//                if (stream->send(tag, "%ac", tmp, 10) == -1) {
                    fprintf(stderr, "BE: stream::send(%%d) failure in FLOW_START_PHASE\n");
                    tag = FLOW_EXIT;
                    break;
                }
                if (stream->flush() == -1) {
                    fprintf(stderr, "BE: stream::flush() failure in FLOW_START_PHASE\n");
                    break;
                }
//                delete out_buffer;
                fflush(stdout);
                sleep(2); // stagger sends
            }
                break;

            case FLOW_EXIT: {
                if (stream->send(tag, "%d", 0) == -1) {
                    fprintf(stderr, "BE: stream::send(%%s) failure in FLOW_EXIT\n");
                    break;
                }
                if (stream->flush() == -1) {
                    fprintf(stderr, "BE: stream::flush() failure in FLOW_EXIT\n");
                }
            }
                break;

            default: {
                fprintf(stderr, "BE: Unknown Protocol: %d\n", tag);
                tag = FLOW_EXIT;
            }
                break;
        }

        fflush(stderr);

    }


}

MRNetBEOutOperator::~MRNetBEOutOperator() {
    if (stream != NULL) {
        while (!stream->is_Closed())
            sleep(1);

        delete stream;
    }
    delete net;
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