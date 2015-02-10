#pragma once
#include "sight_common_internal.h"
#include "comp_shared_ptr.h"
#include <vector>
#include <map>
#include <assert.h>
#include "schema.h"
#include "data.h"
#include <boost/enable_shared_from_this.hpp>
#include <boost/thread/tss.hpp>

class Operator;
typedef SharedPtr<Operator> OperatorPtr;

class Stream;
typedef SharedPtr<Stream> StreamPtr;

class Stream {
  friend class Operator;
  
  private:
  // Life-cycle for Streams:
  // - Stream is created with a given Schema
  //   At this point the Stream is connected to the outgoing port of some Operator but the stream
  //   is not aware of its identity. To communicate the Operator will call the Stream's transfer() function.
  // - In Stream::connect() Stream is connected to the incoming port of some Operator. The Stream is 
  //   aware of the operator's identity and not the index of the incoming port to which it is attached. 
  //   The Operator is aware of the stream and its port index.
  // - To communicate Stream::transfer() calls Operator::recv(), providing it both its input port index
  //   and the Data object that was communicated.
  // - When an Operator's outgoing data has completed, it calls Stream::streamFinished(). In turn, the Stream
  //   calls Operator::inStreamFinished() on its Operator.
  
  // The schema of this Stream's data
  SchemaPtr schema;
  
  // The operator where this stream sends its data
  OperatorPtr targetOp;
  
  // The input port at targetOp where this stream terminates
  unsigned int opInPort;
  
  public:
  Stream(SchemaPtr schema);
  
  // Connects this Stream to the given incoming port of the given Operator
  void connectToOperatorInput(OperatorPtr targetOp, unsigned int opInPort);
  
  // Called by the stream's source operator to communicate the given Data object
  void transfer(DataPtr obj);

  // Called by the stream's source operator to indicate that no more data will be sent on this stream
  void streamFinished();

  //assign a schema dynamically
  void setSchema(SchemaPtr alt_schema);
  
  // Returns this Stream's Schema
  SchemaPtr getSchema() const { return schema; }
  
  // Return whether this object is identical to that object
  bool operator==(const StreamPtr& that) const;
  bool operator!=(const StreamPtr& that) const { return !((*this) == that); }
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const StreamPtr& that) const;
  bool operator<=(const StreamPtr& that) const { return (*this) == that || (*this) < that; }
  bool operator> (const StreamPtr& that) const { return !((*this) < that); }
  bool operator>=(const StreamPtr& that) const { return (*this) == that || (*this) > that; }
}; // class Stream

extern StreamPtr NULLStream;

class Operator: public boost::enable_shared_from_this<Operator> {
  friend class Stream;

  protected:
  
  // This operator's unique ID;
  unsigned int ID;
  
  // The number of input and output streams
  unsigned int numInputs;
  unsigned int numOutputs;
  
  // Vectors of all the incoming and outgoing streams
  std::vector<StreamPtr> inStreams;
  int numConnectedStreams; // Number of input streams that have been connected
  std::vector<StreamPtr> outStreams;
  
  public:
  // Initialization procedure for an Operator:
  // - Creation: where only the dispatch type and the number of inputs and outputs is specified
  // - The input streams are connected via calls to inConnect(). Each stream's schema is 
  //   available for the operator to examine at this point.
  //   inConnect() is implemented in base Operator class.
  // - The inConnectionsComplete() call signals that all the input streams have been connected.
  //   This call returns a vector of Schemas for the output streams.
  //   inConnectionsComplete() must be implemented in classes that derive from Operator.
  // - The output streams are connected via calls to outConnect(). Their schemas must be identical
  //   to those returned by inConnectionsComplete().
  //   outConnect() is implemented in base Operator class.
  // - The outConnectionsComplete() call signals that all the output streams have been connected.
  //   outConnectionsComplete() may be implemented in classes that derive from Operator but is optional.
  // - The operator's work function is called according to the rules of the dispatch policy.
  //   work() must be implemented in classes that derive from Operator.
  
  Operator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID);

  // Loads the Operator from its serialized representation
  Operator(properties::iterator props);

  ~Operator();

	unsigned int getID() const { return ID; }
	unsigned int getNumInputs()  const { return numInputs; }
	unsigned int getNumOutputs() const { return numOutputs; }
  const std::vector<StreamPtr>& getInStreams()  const { return inStreams; }
  const std::vector<StreamPtr>& getOutStreams() const { return outStreams; }

  // Connects the given stream to the given incoming stream index. This function may
  // not be called multiple times for the same index.
  void inConnect(unsigned int idx, StreamPtr s);
  
  // Called to signal that all the incoming streams have been connected. Returns the schemas
  // of the outgoing streams based on the schemas of the incoming streams.
  virtual std::vector<SchemaPtr> inConnectionsComplete()=0;
  
  // Connects the given stream to the given incoming stream index. This function may
  // not be called multiple times for the same index.
  void outConnect(unsigned int idx, StreamPtr s);
  
  // Called to signal that all the outgoing streams have been connected. 
  // After this call the operator's work() function may be called. 
  virtual void outConnectionsComplete() {}
  
  // Called by an incoming Stream to communicate the given Data object
  virtual void recv(unsigned int inStreamIdx, DataPtr obj)=0;
  
  // Called by an incoming Stream to indicate that the incoming stream at this index will send no more data
  virtual void streamFinished(unsigned int inStreamIdx)=0;
  
  // Write a human-readable string representation of this Operator to the given output stream
  virtual std::ostream& str(std::ostream& out) const;
    
  // Return whether this object is identical to that object
  bool operator==(const OperatorPtr& that) const;
  bool operator!=(const OperatorPtr& that) const { return !((*this) == that); }
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const OperatorPtr& that) const;
  bool operator<=(const OperatorPtr& that) const { return (*this) == that || (*this) < that; }
  bool operator> (const OperatorPtr& that) const { return !((*this) < that); }
  bool operator>=(const OperatorPtr& that) const { return (*this) == that || (*this) > that; }
}; // class Operator

extern OperatorPtr NULLOperator;

// Operators and Streams between them are set up based on a configuration file. While it is possible
// manually write such files, this requires knowledge of their format, which may also change over time.
// A simpler mechanism is to create Config objects that generate properties objects that handle all
// serialization and deserialization tasks. 
// Operator structure is managed by OperatorConfig objects. For each class that derives from Operator 
//   there should be a corresponding Config class that derives from OperatorConfig. Each constructor of
//   a Config class takes a pointer to a properties object as an argument, which defaults to NULL.
//   The constructor then calls a static setProperties() method that is implements, which  takes 
//   this pointer as an argument. setProperties() adds the key->value pairs specific to its class to
//   the given properties object, creating a fresh one if it got a NULL pointer. It then returns a pointer
//   to the properties object, which is propagated as an argument to the parent class constructor.
//   When the process reaches the constructor for the base OperatorConfig class we have a fully formed
//   properties object that has separate entries for all the classes up the inheritance stack, ordered from
//   the most derived to base class. This properties object can then be serialized and its serial
//   representation can be deserialized to create fresh Operator objects.
// Streams between operators are managed by StreamConfig objects that maintain a properties object that encodes
//   the IDs of the stream's source and target, as well as the output/input ports on the source/target
//   that they connect.
class OperatorConfig {
  public:
  propertiesPtr props;
  
  OperatorConfig(unsigned int numInputs, unsigned int numOutputs, unsigned int ID, propertiesPtr props=NULLProperties);
}; // OperatorConfig

class StreamConfig {
  public:
  properties props;
  
  StreamConfig(unsigned int fromOpID, unsigned int fromOpPort, unsigned int toOpID, unsigned int toOpPort);
}; // class StreamConfig

// Mapping of unique operator names to functions that create OperatorPtrs from their serialized representation
class OperatorRegistry {
  public:
  typedef OperatorPtr (*creator)(properties::iterator props);
//  static std::map<std::string, creator> creators;

  static boost::thread_specific_ptr< std::map<std::string, creator> > creatorsInstance;

  static boost::thread_specific_ptr< std::map<std::string, creator> >& getCreators(){
      if(!creatorsInstance.get()){
          creatorsInstance.reset(new std::map<std::string, creator>());
      }
      return creatorsInstance;
  }
  // Maps the given label to a creator function, returning whether this mapping overrides a prior one (true) or is
  // a fresh mapping (false).	
  static bool regCreator(const std::string& label, creator create);
  
  static OperatorPtr create(propertiesPtr props);
}; // OperatorRegistry

// Operator where the work function is dispatched synchonously: when a single Data object
// arrives on ALL the incoming streams.
class SynchOperator: public Operator {
  public:
  SynchOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID);
  
  // Loads the Operator from its serialized representation
  SynchOperator(properties::iterator props);
  
  void init();
  
  // Called when a tuple arrives on all the incoming streams. 
  // inData: holds the Data object from each stream.
  // This function may send Data objects on some of the outgoing streams.
  virtual void work(const std::vector<DataPtr>& inData)=0;
  
  // Called to inform the operator that no more data will be communicated on any of the incoming streams
  virtual void inStreamsFinished() {}
  
  private:
  // list of vectors of all that Data objects that have arrived on incoming streams.
  // The first element contains the most recently arrived objects, the second contains
  // the second most recently, etc.
  std::list<std::vector<DataPtr> > incomingData;
  
  // Records the number of Data objects in incomingData from each incoming Stream
  std::vector<unsigned int> numDataFromInStream;
  
  // Records the iterator to the numDataFromInStream[i]^th element in incomingData
  // for each incoming stream i
  std::vector<std::list<std::vector<DataPtr> >::iterator> curIncomingDataIter;
  
  // Records the maximum values in numDataFromInStream among all incoming streams
  unsigned int maxNumDataFromInStream;
  //unsigned int minNumDataFromInStream;
  
  // Records the number of streams on which at least one Data object has arrived
  unsigned int numStreamsWithData;

  //list indicating
  vector<unsigned int> unFinishedStreams;
  
  public:
  // Called by an incoming Stream to communicate the given Data object
  void recv(unsigned int inStreamIdx, DataPtr obj);
  
  private:
  // Records whether any of the incoming streams have finished sending data
  bool finishedOperator;
  
  // Called by Stream to indicate that the incoming stream at this index will send no more data
  void streamFinished(unsigned int inStreamIdx);
}; // class SynchOperator

// Operator where the work function is dispatched asynchonously: when a single Data object
// arrives on ANY one incoming stream.
class AsynchOperator: public Operator {
  public:
  AsynchOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID);

  // Loads the Operator from its serialized representation
  AsynchOperator(properties::iterator props);
  
  // Called when a tuple arrives on single incoming stream. 
  // inStreamIdx: the index of the stream on which the object arrived
  // inData: holds the single Data object from the single stream
  // This function may send Data objects on some of the outgoing streams.
  virtual void work(unsigned int inStreamIdx, DataPtr inData)=0;

  // Called by an incoming Stream to communicate the given Data object
  void recv(unsigned int inStreamIdx, DataPtr obj);

  // Called to inform the operator that no more data will be communicated on the given incoming stream
  virtual void inStreamFinished(unsigned int inStreamIdx) {}
  
  // Called by Stream to indicate that the incoming stream at this index will send no more data
  void streamFinished(unsigned int inStreamIdx);
}; // class AsynchOperator

// Operator that has no inputs and thus, the work function is invoked exactly once after
// the operator is created since no Data objects will ever arrive to trigger work.
class SourceOperator: public Operator {
  public:
  SourceOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID);

  // Loads the Operator from its serialized representation
  SourceOperator(properties::iterator props);
  
  // Called after the outputs of this Operator have been initialized.
  // The function is expected to return when there is no more data to be processed.
  // The implementation does not have to call streamFinished() on the outgoing streams as this
  // will be done automatically by the SourceOperator base class.
  virtual void work()=0;
  
  // Called after the outputs of this Operator have been initialized. This function
  // is implemented by SourceOperator and calls work(), which is implemented by derived
  // classes. It also calls streamFinished() on the outgoing streams when work() exits.
  void driver();

  // Called by an incoming Stream to communicate the given Data object
  void recv(unsigned int inStreamIdx, DataPtr obj) {}
  
  // Called by an incoming Stream to indicate that the incoming stream at this index will send no more data
  void streamFinished(unsigned int inStreamIdx) {}
}; // class SourceOperator

// Operator that reads Data objects from a given FILE* using a given Schema
class InFileOperator : public SourceOperator {
  private:
  SchemaPtr schema;
  FILE* inFile;
  
  // Records whether we need to close the file in the destructor or whether it will be destroyed by users of this Operator
  bool closeFile;
  
  public:
  // inFile: points to the FILE from which we'll read data
  // schema: the schema of the data from inFile
  InFileOperator(unsigned int ID, FILE* inFile, SchemaPtr schema);
  
  // inFName: the name of the file from which we'll read data
  // schema: the schema of the data from inFile
  InFileOperator(unsigned int ID, const char* inFName, SchemaPtr schema);

  // Loads the Operator from its serialized representation
  InFileOperator(properties::iterator props);
  
  // Creates an instance of the Operator from its serialized representation
  static OperatorPtr create(properties::iterator props);
  
  ~InFileOperator();
  
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

class InFileOperatorConfig: public OperatorConfig {
  public:
  InFileOperatorConfig(unsigned int ID, const char* inFName, SchemaConfigPtr schemaCfg, propertiesPtr props=NULLProperties);

  static propertiesPtr setProperties(const char* inFName, SchemaConfigPtr schemaCfg, propertiesPtr props);
}; // class InFileOperatorConfig

// Operator that writes received Data objects to a given FILE* using the Schema of its single input stream
class OutFileOperator : public AsynchOperator {
  private:
  FILE* outFile;

  // Records whether we need to close the file in the destructor or whether it will be destroyed by users of this Operator
  bool closeFile;
  
  public:
  // outFile: points to the FILE to which we'll write data
  OutFileOperator(unsigned int ID, FILE* outFile);

  // outFName: the name of the file to which we'll write data
  OutFileOperator(unsigned int ID, const char* outFName);

  // Loads the Operator from its serialized representation
  OutFileOperator(properties::iterator props);
  
  // Creates an instance of the Operator from its serialized representation
  static OperatorPtr create(properties::iterator props);
  
  ~OutFileOperator();
  
  // Called to signal that all the incoming streams have been connected. Returns the schemas
  // of the outgoing streams based on the schemas of the incoming streams.
  std::vector<SchemaPtr> inConnectionsComplete();

  // Called when a tuple arrives on single incoming stream. 
  // inStreamIdx: the index of the stream on which the object arrived
  // inData: holds the single Data object from the single stream
  // This function may send Data objects on some of the outgoing streams.
  void work(unsigned int inStreamIdx, DataPtr inData);
  
  // Write a human-readable string representation of this Operator to the given output stream
  virtual std::ostream& str(std::ostream& out) const;
}; // class OutFileOperator
typedef SharedPtr<OutFileOperator> OutFileOperatorPtr;

class OutFileOperatorConfig: public OperatorConfig {
  public:
  OutFileOperatorConfig(unsigned int ID, const char* outFName, propertiesPtr props=NULLProperties);
  
  static propertiesPtr setProperties(const char* outFName, propertiesPtr props);
}; // class OutFileOperatorConfig


// Operator that computes the join of the KeyValMap objects on all the incoming streams and
// emits ExplicitKeyValMap objects for each joined key->value pair
class SynchedKeyValJoinOperator : public SynchOperator {
  private:
  // The schema of the incoming streams. All streams must use the same schema.
  KeyValSchemaPtr schema;
  
  // This Operator will emit ExplicitKeyValueMaps that map the keys of its incoming objects
  // to tuples of their values. This is the schema of those tuples.
  TupleSchemaPtr valTupleSchema;
  
  // The schema of the key->value mappings that will be emitted by this operator
  ExplicitKeyValSchemaPtr outputKeyValSchema;

  public:
  SynchedKeyValJoinOperator(unsigned int numInputs, unsigned int ID);

  // Loads the Operator from its serialized representation
  SynchedKeyValJoinOperator(properties::iterator props);
  
  // Creates an instance of the Operator from its serialized representation
  static OperatorPtr create(properties::iterator props);

  virtual void inStreamsFinished();
  
  ~SynchedKeyValJoinOperator();
  
  // Called to signal that all the incoming streams have been connected. Returns the schemas
  // of the outgoing streams based on the schemas of the incoming streams.
  std::vector<SchemaPtr> inConnectionsComplete();

  // Functor that can be applied to all the key->value mappings 
  // in a set of KeyValMap in the intersection of these maps' keys
  class joinMapFunc: public KeyValMap::commonMapFunc {
    SynchedKeyValJoinOperator& parent;
    
    // The key->value mapping that will be produced as a result of this map operation
    ExplicitKeyValMapPtr eKeyVal;
    
    public:
    joinMapFunc(SynchedKeyValJoinOperator& parent);
    
    // Called on every key->value pair in a set of maps, 
    // where the key is identical
    // key - the key that is shared by the pairs in the different maps
    // values - vector of all the values from the different maps
    //    that have this key, one from each map
    void map(const DataPtr& key, 
             const std::vector<DataPtr>& values);

    // Called when the iteration has completed
    void iterComplete();
  }; // class joinMapFunc 

  // Called when a tuple arrives on all the incoming streams. 
  // inData: holds the Data object from each stream.
  // This function may send Data objects on some of the outgoing streams.
  void work(const std::vector<DataPtr>& inData);
  
  // Write a human-readable string representation of this Operator to the given output stream
  std::ostream& str(std::ostream& out) const;
}; // class SynchedKeyValJoinOperator

class SynchedKeyValJoinOperatorConfig: public OperatorConfig {
  public:
  SynchedKeyValJoinOperatorConfig(unsigned int numInputs, unsigned int ID, 
                                  propertiesPtr props=NULLProperties);
  
  static propertiesPtr setProperties(propertiesPtr props);
}; // class SynchedKeyValJoinOperatorConfig

// Operator that scatters the data objects arriving on a single input stream among its output streams.
class ScatterOperator : public AsynchOperator {
  private:
  // The index of the output stream that the next data object will be sent to
  unsigned int outStreamIdx;
  
  public:
  ScatterOperator(unsigned int numOutputs, unsigned int ID);

  // Loads the Operator from its serialized representation
  ScatterOperator(properties::iterator props);
  
  // Creates an instance of the Operator from its serialized representation
  static OperatorPtr create(properties::iterator props);
  
  ~ScatterOperator();
  
  // Called to signal that all the incoming streams have been connected. Returns the schemas
  // of the outgoing streams based on the schemas of the incoming streams.
  std::vector<SchemaPtr> inConnectionsComplete();

  // Called when a tuple arrives on single incoming stream. 
  // inStreamIdx: the index of the stream on which the object arrived
  // inData: holds the single Data object from the single stream
  // This function may send Data objects on some of the outgoing streams.
  void work(unsigned int inStreamIdx, DataPtr inData);
  
  // Write a human-readable string representation of this Operator to the given output stream
  virtual std::ostream& str(std::ostream& out) const;
}; // class ScatterOperator

class ScatterOperatorConfig: public OperatorConfig {
  public:
  ScatterOperatorConfig(unsigned int numOutputs, unsigned int ID, propertiesPtr props=NULLProperties);
  
  static propertiesPtr setProperties(propertiesPtr props);
}; // class ScatterOperatorConfig


/*
	- Operators:
		? Input is a fixed number of streams of data items
			� Synchronous: item must arrive on all incoming streams before operation is called
			� Asynchronous: operation is called when one or more items arrive, the number of available items is up to the system
		? Output is a single stream of data items
			� Operators are not required to emit data on streams and may instead keep a local cache of data before emitting something
		? Each input/output data item is an instance of CtxtObsMap and may cover any number of context->observations and may be lossy or lossless
		? 
		? API
			� class Operator {
			  typef enum {synch, asynch} synchType;
			  synchType synchrony;
			  SchemaPtr schema;
			  Operator(synchType synchrony, SchemaPtr schema) : synchrony(synchrony), schema(schema) {}
			
			  // Called to process data on incoming streams. 
			  // inStreams: vector that contains for each incoming stream a list of Data objects that have arrived on that stream. 
			  //   If synchrony==synch it is guaranteed that inStreams contains exactly one Data object on each incoming stream
			  virtual void process(std::vector<std::list<const CtxtObsMap&> > inStreams)=0;
			  void emit(const CtxtObsMap& data);
};
			� class JoinOp: public Operator {
			  // All the context->observation pairs ever observed on each incoming stream
			  std::vector<ExplicitCtxtObsMapPtr> history;
			  
			  void process(const std::vector<std::list<const CtxtObsMapPtr&> >& inStreams) {
			    // Run each context->observation map over the history, looking for common contexts
			    for(int i=0; i<inStreams.size(); i++) {
			      for(list<const CtxtObsMapPtr&>::const_iterator inCP=inStreams[i].begin(); inCP!=inStreams[i].end(); inCP++) {
			        // Attempt to join this map with the histories on other streams (!=I)
			        
			        // First, create a vector such just the histories on other streams
			        vector<ExplicitCtxtObsMapPtr> otherHists(history.size()-1);
			        for(int j=0, cnt=0; j!=history.size(); j++) {
			          if(i==j) continue;
			          otherHists[cnt] = history[j];
			          cnt++;
			        }
			
			        // Next, use iterate the joinMap functor over all the context->observation pairs that inCP shares with all pairs in otherHists
			  class joinMap: public commonMapFunc {
			      // Reference to the JoinOp that hosts this mapper
			      JoinOp& host;
			      commonMapFunc(JoinOp& host) : host(host) {}
			
			      // Called on every context->observation pair in a set of maps, where the context is identical
			      void map(const TuplePtr& context, const std::vector<const TuplePtr&> observations) {
			        // Emits a tuple on the outgoing stream for all the maps with common contexts
			        host.emit(boost::make_shared<ExplicitCtxtObsMap>(context, boost::make_shared<TuplePtr>(observations)));
			      }
			    }
			      // Called when the iteration has completed
			      void iterComplete() {}
			  }; // class commonMapFunc 
			       inCP->alignMap(joinMap(*this), otherHists); 
			
			         }
			        }
			      }
			    }
			  }
			� // Join that looks for keys that are similar within a given threshold
			class JoinSimilarOp: public Operator {
			};
			� // Given multiple streams with the same schema this operator produces a unified stream that contains
			// context->observation pairs from all the streams
			class Aggregate: public Operator {
			
			};
			� // Emit all incoming context->observation pair for which a given condition holds
			class Select: public Operator {
			
			};
			� // Adds new dimension to context or observation that is a function of existing contexts or 
			// observations and emits the resulting richer tuples
			class AddDerivedData: public Operator {
			
			};
			� // Removes a given dimension of context or observation from the incoming events and emits the 
			// resulting stripped tuples
			class RemoveData: public Operator {
			};
			� // Given incoming events where the one of the observation dimensions is a CtxtObsMap, moves its
			// context to the event's context and moves its observations to the event's observation
			class Flatten: public Operator {
			
			};
			� // Takes selected dimensions of the incoming events' context and observations and emits events 
// where these dimensions are replaced with an observation CtxtObsMap that contains the removed 
// context and observation dimensions
			class Deepen: public Operator {
			
			};
			� // Applies a given function to the incoming events and emit the event produced by the function
			// Functions can:
			// - Emit events each time they're applied and/or
			// - Accumulate some metric and emit it periodically
class Map: public Operator {
			
			};
			� // Perform Lossless compression on the incoming events in using a data-agnostic (uses equality and 
// inequality info) or data-sensitive (uses additional info) manner.
			class LosslessCompress: public Operator {
			
			};
			� // Perform Lossy compression on the incoming events in using a data-agnostic (uses equality and 
// inequality info) or data-sensitive (uses additional info) manner.
			class LossyCompress: public Operator {
			
			};
*/