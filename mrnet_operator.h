#pragma once
#include "operator.h"
#include "mrnet/MRNet.h"
#include "mrnet/Packet.h"
#include "mrnet/NetworkTopology.h"
#include <time.h>
#include <chrono>

//using namespace MRN ;
/*****************************************
* MRNet specific operator configurations
************************************** */

/*****************************************
* MRNet Frontend config
*****************************************/
/*
[|MRNetFrontSource numProperties="1" name0="topology_file" val0="filepath..." name1="backend_exec" val0="exe path..."
        name2="so_file" val2="filepath..."      ]
[Operator numProperties="3" name0="ID" val0="0" name1="numInputs" val1="0" name2="numOutputs" val2="1"]
[| schema ...]
 ....
 ....
[/schema]
[/MRNetFrontSource]

*/

class MRNetFrontSourceOperatorConfig: public OperatorConfig {
public:
    MRNetFrontSourceOperatorConfig(unsigned int ID,  const char* topology_file,
            const char* backend_exec, const char* so_file, SchemaConfigPtr schemaCfg, propertiesPtr props=NULLProperties);

    static propertiesPtr setProperties( const char* topology_file,
            const char* backend_exec, const char* so_file, SchemaConfigPtr schemaCfg, propertiesPtr props);
};


/*****************************************
* MRNet Filter Source config
*****************************************/
/*
[|MRNetFilterSource numProperties="1" name0="topology_file" val0="filepath..." name1="backend_exec" val0="exe path..."
        name2="so_file" val2="filepath..."      ]
[Operator numProperties="3" name0="ID" val0="0" name1="numInputs" val1="0" name2="numOutputs" val2="n"]
[| schema ...]
 ....
 ....
[/schema]
[/MRNetFilterSource]

*/

class MRNetFilterSourceOperatorConfig: public OperatorConfig {
public:
    MRNetFilterSourceOperatorConfig(unsigned int ID, unsigned int num_outgoing_streams, SchemaConfigPtr schemaCfg, propertiesPtr props=NULLProperties);

    static propertiesPtr setProperties(SchemaConfigPtr schemaCfg, propertiesPtr props);
};


/*****************************************
* MRNet Filter Sink config
*****************************************/
/*
*
[|MRNetFilterOut numProperties="1" ]
[Operator numProperties="3" name0="ID" val0="3" name1="numInputs" val1="1" name2="numOutputs" val2="0"]
[/MRNetFilterOut]


[|MRNetBackOut numProperties="1" ]
[Operator numProperties="3" name0="ID" val0="3" name1="numInputs" val1="1" name2="numOutputs" val2="0"]
[/MRNetBackOut]

* */

class MRNetFilterOutOperatorConfig: public OperatorConfig {
public:
    MRNetFilterOutOperatorConfig(unsigned int ID,propertiesPtr props=NULLProperties);

    static propertiesPtr setProperties(propertiesPtr props);
};

/*****************************************
* MRNet Backend Sink config
*****************************************/

class MRNetBackendOutOperatorConfig : public OperatorConfig {
public:
    MRNetBackendOutOperatorConfig(unsigned int ID, propertiesPtr props=NULLProperties);

    static propertiesPtr setProperties(propertiesPtr props);
};


/*
* MRNet specific operators that function on MRNet streams - sole role of the following set of classes are to handle
* MRNet streams and produce Dataptr objects (deseraialize) recieved from downstream so that other operators can function
* on them and also to push resulting data packets (serialize) upstream on MRNet  Tree
* */

class MRNetInfo{
public:
    MRN::Network *net;
    int stream_id;
    int tag_id;
    MRN::Stream *stream;
    std::set< MRN::Rank > peers;
    std::vector< MRN::PacketPtr > *packets_in;
    std::vector< MRN::PacketPtr > *packets_out;

}  ;
/*
*
*MRNetFilterSourceOperator
* -num inputs = 0 -num outputs = n
* specification
* - takes incoming data packet from MRNet leaf/comm node via MRNet Filter and produce deserialized data object/dataPtr
* - use schema to desrialize binary data coming in
* - dataPtr is passed through specific stream correspoding to an incoming MRNet child stream
*  (it is the role of consequent operators to operate on these output streams )
*
* */

class MRNetFilterSourceOperator : public SourceOperator {
private:
    SchemaPtr schema;
    StreamBuffer * streamBuf;
    MRNetInfo mrn_info;
    std::map<unsigned int, unsigned int> out_ranks;
    unsigned int curr_assignment;    
	

private:
    void _registerRank(unsigned int inrank){
        if(out_ranks.find(inrank) == out_ranks.end()){
            out_ranks.insert(std::pair<unsigned int ,unsigned int >(inrank, curr_assignment++));
        }
    };

    int _getAssignment(unsigned int inrank){
        return (int) out_ranks[inrank];
    };

public:
    // Loads the Operator from its serialized representation
    MRNetFilterSourceOperator(properties::iterator props);

    // Creates an instance of the Operator from its serialized representation
    static OperatorPtr create(properties::iterator props);

    ~MRNetFilterSourceOperator();

    int findOutStream(unsigned int inrank); 
    // Called to signal that all the incoming streams have been connected. Returns the schemas
    // of the outgoing streams based on the schemas of the incoming streams.
    std::vector<SchemaPtr> inConnectionsComplete();

    // Called to signal that all the outgoing streams have been connected.
    // After this call the operator's work() function may be called.
    void outConnectionsComplete();

    void setMRNetInfoObject(MRNetInfo &inf);

    // Called after the outputs of this Operator have been initialized.
    // The function is expected to return when there is no more data to be processed.
    // The implementation does not have to call streamFinished() on the outgoing streams as this
    // will be done automatically by the SourceOperator base class.
    void work();

    // Write a human-readable string representation of this Operator to the given output stream
    virtual std::ostream& str(std::ostream& out) const;
}; // class InFileOperator

/*
*
*MRNetFilterSinkOperator
* -num inputs = 1 -num outputs = 1
* specification
* - takes incoming data object/dataPtrs from Flow and serializes them into MRNet out data packets
* - use schema from incoming stream to serialize data
* - use schema to deserialize binary data coming in through a stream
*
* */

// Operator that writes received Data objects to a MRNet upstream, using the Schema of its single input stream
class MRNetFilterOutOperator : public AsynchOperator {
private:
    // Records whether we need to close the file in the destructor or whether it will be destroyed by users of this Operator
    std::vector<MRN::PacketPtr> *packets_out;
    MRNetInfo mrn_info;

public:
    // Loads the Operator from its serialized representation
    MRNetFilterOutOperator(properties::iterator props);

    // Creates an instance of the Operator from its serialized representation
    static OperatorPtr create(properties::iterator props);

    ~MRNetFilterOutOperator();


    void setMRNetInfoObject(MRNetInfo &inf);
    // Called to signal that all the incoming streams have been connected. Returns the schemas
    // of the outgoing streams based on the schemas of the incoming streams.
    std::vector<SchemaPtr> inConnectionsComplete();

    // Called when a tuple arrives on single incoming stream.
    // inStreamIdx: the index of the stream on which the object arrived
    // inData: holds the single Data object from the single stream
    // This function may send Data objects on some of the outgoing streams.
    virtual void work(unsigned int inStreamIdx, DataPtr inData);

    virtual void inStreamFinished(unsigned int inStreamIdx);
    // Write a human-readable string representation of this Operator to the given output stream
    virtual std::ostream& str(std::ostream& out) const;
};

/*
*
*MRNetBESinkOperator
* -num inputs = 1 -num outputs = 1
* specification
* - takes incoming data object/dataPtrs from Flow (usually File source -> MRNet BE sink) and serializes them into
* MRNet out data packets
* - use schema from incoming stream to serialize data
* - use schema to deserialize binary data coming in
*
* */
//definition of BE params

class MRNetBEOutOperator : public AsynchOperator {
private:
    MRN::Stream *stream ;
    MRN::Network *net ;
    bool init;
public:
    // Loads the Operator from its serialized representation
    MRNetBEOutOperator(properties::iterator props);

    // Creates an instance of the Operator from its serialized representation
    static OperatorPtr create(properties::iterator props);

    ~MRNetBEOutOperator();

    // Called to signal that all the incoming streams have been connected. Returns the schemas
    // of the outgoing streams based on the schemas of the incoming streams.
    std::vector<SchemaPtr> inConnectionsComplete();

    // Called when a tuple arrives on single incoming stream.
    // inStreamIdx: the index of the stream on which the object arrived
    // inData: holds the single Data object from the single stream
    // This function may send Data objects on some of the outgoing streams.
    void work(unsigned int inStreamIdx, DataPtr inData);

    virtual void inStreamFinished(unsigned int inStreamIdx) ;
    // Write a human-readable string representation of this Operator to the given output stream
    virtual std::ostream& str(std::ostream& out) const;
};


/*
*
*MRNetFESourceOperator
* -num inputs = 0 -num outputs = 1 (ie:- outputs == 1 since FE filter reduces data to one stream)
* specification
* - takes incoming data object/dataPtrs from Flow (usually File source -> MRNet BE sink) and serializes them into
* MRNet out data packets
* - use schema to deserialize binary data coming in
*
* */
extern bool saw_failure;


typedef std::chrono::high_resolution_clock::time_point t_pnt ;
using namespace std::chrono;

static inline t_pnt get_wall_t(){
    return high_resolution_clock::now();
};

class MRNetFESourceOperator : public SourceOperator {
private:
    SchemaPtr schema;
    char * topology_file;
    char * backend_exe;
    char * so_file ;
    const char * dummy_argv;
    bool init ;
    int num_backends;
    StreamBuffer * streamBuf;

    //MRNet specific
    MRN::Network * net;
    MRN::Stream * active_stream;
    MRN::Communicator *comm_BC;
   
    double dt;
   

public:
    // Loads the Operator from its serialized representation
    MRNetFESourceOperator(properties::iterator props);

    int initMRNet();
    static void Failure_Callback( MRN::Event* evt, void* )  ;
    // Creates an instance of the Operator from its serialized representation
    static OperatorPtr create(properties::iterator props);

    ~MRNetFESourceOperator();

    // Called to signal that all the incoming streams have been connected. Returns the schemas
    // of the outgoing streams based on the schemas of the incoming streams.
    std::vector<SchemaPtr> inConnectionsComplete();

    // Called to signal that all the outgoing streams have been connected.
    // After this call the operator's work() function may be called.
    void outConnectionsComplete();

    // Called after the outputs of this Operator have been initialized.
    // The function is expected to return when there is no more data to be processed.
    // The implementation does not have to call streamFinished() on the outgoing streams as this
    // will be done automatically by the SourceOperator base class.
    void work();

    // Write a human-readable string representation of this Operator to the given output stream
    virtual std::ostream& str(std::ostream& out) const;
}; // class InFileOperator


/*
* MRNet Flow Initializer  - flow initializer sequence is different from front, filter , backend
* This class will iniitalize Flow for each scenario
*
* */
class MRNetFlowInitializer{
private:
    static void init_common();

public:
    /*
    * Initialize front end flow within the MRNet front end initialization
    * Flow has MRNetFESourceOperator -> MRNetFileOutOperator
    * */
    static void init_mrnet_fe_flow();

    /*
    * Initialize filter flow within the filter once.
    * Filter has MRNetFilterSourceOperator-> operators... -> MRNetFilterOutOperator
    * */
    static void init_mrnet_filter_flow();

    /*
   * Initialize back end flow within the MRNet back end initialization
   * Flow has InfileOperator -> MRNetBEOutOperator
   * */
    static void init_mrnet_backend_flow();

};
