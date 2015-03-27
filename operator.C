#include "operator.h"
#include "mrnet_flow.h"
#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <math.h>

using namespace std;

/******************
 ***** Stream *****
 ******************/

Stream::Stream(SchemaPtr schema): schema(schema) {}

// Connects this Stream to the given incoming port of the given Operator
void Stream::connectToOperatorInput(OperatorPtr targetOp, unsigned int opInPort) {
  this->targetOp = targetOp;
  this->opInPort = opInPort;
}

// Called by the stream's source operator to communicate the given Data object
void Stream::transfer(DataPtr obj) {
  assert(targetOp);
//  cout << "Stream::transfer(opInPort="<<opInPort<<") "; obj->str(cout, schema); cout << endl;
//  cout << "targetOp="<<targetOp.get()<<endl;
  targetOp->recv(opInPort, obj);  
}

// Called by the stream's source operator to indicate that no more data will be sent on this stream
void Stream::streamFinished() {
  targetOp->streamFinished(opInPort);
}

void Stream::setSchema(SchemaPtr alt_schema){
//    schema = dynamicPtrCast<SchemaPtr>(alt_schema);
    printf("schema  shared_ptr->px : %p \n", schema.get() );
    schema = alt_schema;
}

// Return whether this object is identical to that object
bool Stream::operator==(const StreamPtr& that) const
{ return (targetOp==that->targetOp && opInPort==that->opInPort); }

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool Stream::operator<(const StreamPtr& that) const
{ return  targetOp< that->targetOp ||
         (targetOp==that->targetOp && opInPort<that->opInPort); }

StreamPtr NULLStream;

/********************
 ***** Operator *****
 ********************/

Operator::Operator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID): 
  numInputs(numInputs), numOutputs(numOutputs), ID(ID) {
  inStreams.resize(numInputs);
  outStreams.resize(numOutputs);
}

Operator::Operator(properties::iterator props) {
	assert(props.name()=="Operator");
	numInputs  = props.getInt("numInputs");
	numOutputs = props.getInt("numOutputs");
	ID         = props.getInt("ID");
	inStreams.resize(numInputs);
    outStreams.resize(numOutputs);
}

Operator::~Operator() {
  // We now empty out our shared_ptr-based data structures to remove all dependence
  // cycles and allow them to be deallocated via the shared_ptr reference counting mechanism.
  
  // Delete all the incoming streams
  inStreams.clear();
  
  // Remove all the outgoing streams from the inStreams vector of their destination Operators
  for(vector<StreamPtr>::iterator out=outStreams.begin(); out!=outStreams.end(); ++out) {
    (*out)->targetOp->inStreams[(*out)->opInPort] = NULLStream;
  }
}

// Connects the given stream to the given incoming stream index. This function may
// not be called multiple times for the same index.
void Operator::inConnect(unsigned int idx, StreamPtr s) {
  assert(idx < numInputs);
  inStreams[idx] = s;

  // Connect the stream to an input port of this operator
  s->connectToOperatorInput(shared_from_this(), idx);
}

// Connects the given stream to the given incoming stream index. This function may
// not be called multiple times for the same index.
void Operator::outConnect(unsigned int idx, StreamPtr s) {
  assert(idx < numOutputs);
  outStreams[idx] = s;
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& Operator::str(std::ostream& out) const {
  out << "[Operator: ID="<<ID<<", #in="<<numInputs<<", #out="<<numOutputs<<"]";
  return out;
}

// Return whether this object is identical to that object
bool Operator::operator==(const OperatorPtr& that) const
{ return ID==that->ID; }

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool Operator::operator<(const OperatorPtr& that) const
{ return  ID< that->ID; }

OperatorPtr NULLOperator;

/**************************
 ***** OperatorConfig *****
 **************************/
OperatorConfig::OperatorConfig(unsigned int numInputs, unsigned int numOutputs, unsigned int ID, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["numInputs"]  = txt()<<numInputs;
  pMap["numOutputs"] = txt()<<numOutputs;
  pMap["ID"]         = txt()<<ID;
  
  props->add("Operator", pMap);
  
  this->props = props;
}

/************************
 ***** StreamConfig *****
 ************************/
 
StreamConfig::StreamConfig(unsigned int fromOpID, unsigned int fromOpPort, unsigned int toOpID, unsigned int toOpPort) {
  map<string, string> pMap;
  pMap["fromOpID"]   = txt()<<fromOpID;
  pMap["fromOpPort"] = txt()<<fromOpPort;
  pMap["toOpID"]     = txt()<<toOpID;
  pMap["toOpPort"]   = txt()<<toOpPort;
  
  props.add("Stream", pMap);
}

/****************************
 ***** OperatorRegistry *****
 ****************************/

//std::map<std::string, OperatorRegistry::creator> OperatorRegistry::creators;

boost::thread_specific_ptr< std::map<std::string, OperatorRegistry::creator> > OperatorRegistry::creatorsInstance;

// Maps the given label to a creator function, returning whether this mapping overrides a prior one (true) or is
// a fresh mapping (false).	
bool OperatorRegistry::regCreator(const std::string& label, OperatorRegistry::creator create) {
//  std::map<std::string, creator>::iterator i=creators.find(label);
//  creators.insert(make_pair(label, create));
//  return i!=creators.end();

  std::map<std::string, creator>::iterator i=(*getCreators()).find(label);
  (*getCreators()).insert(make_pair(label, create));
  return i!=(*getCreators()).end();
}

OperatorPtr OperatorRegistry::create(propertiesPtr props) {
	if((*getCreators()).find(props->name()) == (*getCreators()).end()) { cerr << "ERROR: no deserializer available for "<<props->name()<<"!"<<endl; assert(0); }
  return (*getCreators())[props->name()](props->begin());
}

/*************************
 ***** SynchOperator *****
 *************************/
 
SynchOperator::SynchOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID): Operator(numInputs, numOutputs, ID) {
	init();
}

// Loads the Operator from its serialized representation
SynchOperator::SynchOperator(properties::iterator props) : Operator(props) {
	init();
}

void SynchOperator::init() {
  finishedOperator = false;
  numDataFromInStream.resize(numInputs, 0);
  curIncomingDataIter.resize(numInputs);
  maxNumDataFromInStream=0;
  //minNumDataFromInStream=INT_MAX;
  numStreamsWithData=0;

  for (int i = 0; i < numInputs; i++) {
        unFinishedStreams.push_back((unsigned int) i);
  }
}

// Called by an incoming Stream to communicate the given Data object
void SynchOperator::recv(unsigned int inStreamIdx, DataPtr obj) {
  //cout << "SynchOperator::recv("<<this<<") numDataFromInStream["<<inStreamIdx<<"]="<<numDataFromInStream[inStreamIdx]<<", numStreamsWithData="<<numStreamsWithData<<", maxNumDataFromInStream="<<maxNumDataFromInStream<<endl;
  // If we've not yet reserved an entry for this incoming stream in incomingData
  if(numDataFromInStream[inStreamIdx] >= maxNumDataFromInStream) {
    // Add a fresh vector to the back of incomingData
    incomingData.push_back(std::vector<DataPtr>());
    incomingData.back().resize(numInputs);
  }
  
  // Record the iterator that points to the spot in numDataFromInStream where we'll place obj
  if(numDataFromInStream[inStreamIdx]==0) {
    curIncomingDataIter[inStreamIdx] = incomingData.begin();
    ++numStreamsWithData;
  } else
    ++curIncomingDataIter[inStreamIdx];

  // Place obj in the reserved location
  //cout << "    #incomingData="<<incomingData.size()<<endl;
  (incomingData.back())[inStreamIdx] = obj;
  
  // Update numDataFromInStream, maxNumDataFromInStream and minNumDataFromInStream
  ++numDataFromInStream[inStreamIdx];
  if(numDataFromInStream[inStreamIdx] > maxNumDataFromInStream)
    maxNumDataFromInStream = numDataFromInStream[inStreamIdx];
  /*if(numDataFromInStream[inStreamIdx] < minNumDataFromInStream)
    minNumDataFromInStream = numDataFromInStream[inStreamIdx];*/

  //cout << "    numDataFromInStream["<<inStreamIdx<<"]="<<numDataFromInStream[inStreamIdx]<<", numStreamsWithData="<<numStreamsWithData<<", maxNumDataFromInStream="<<maxNumDataFromInStream<<endl;
  
  assert(numStreamsWithData>0);
  // If we have at least one Data object on each incoming stream
  if(numStreamsWithData == numInputs) {
    //cout << "    work()"<<endl;
    // Call the work function on the vector of Data objects
    work(*incomingData.begin());
    
    // Pull off the oldest data objects from incomingData
    incomingData.pop_front();
    
    // Update numDataFromInStream, maxNumDataFromInStream and minNumDataFromInStream
    maxNumDataFromInStream = 0;
    //minNumDataFromInStream = INT_MAX;
    numStreamsWithData=0;
    for(unsigned int i=0; i<numInputs; ++i) {
      --numDataFromInStream[i];
      if(numDataFromInStream[i] > maxNumDataFromInStream)
        maxNumDataFromInStream = numDataFromInStream[i];
      /*if(numDataFromInStream[i] < minNumDataFromInStream)
        minNumDataFromInStream = numDataFromInStream[i];*/
      if(numDataFromInStream[i]>0)
        ++numStreamsWithData;
      //cout << "         numDataFromInStream["<<i<<"]="<<numDataFromInStream[i]<<", numStreamsWithData="<<numStreamsWithData<<", maxNumDataFromInStream="<<maxNumDataFromInStream<<endl;
    }
    
    // curIncomingDataIter is not updated. Its either the same or if numDataFromInStream[inStreamIdx]==0,
    // its garbage and will not be used.
  }
}

// Called by Stream to indicate that the incoming stream at this index will send no more data
void SynchOperator::streamFinished(unsigned int inStreamIdx) { 
  // This operator stops when any of the incoming streams stop since the work() function 
  // is only called when there's data on all the incoming streams.

  //check if how many streams has finished in sync
  for(vector<unsigned int>::iterator it = unFinishedStreams.begin(); it != unFinishedStreams.end() ;){
      if(*it == inStreamIdx){
          //remove this from list - this stream is done
          #ifdef VERBOSE
          printf("[SyncOperator]: stream finished via inStreamIdx : %u PID : %d \n", *it, getpid());
          #endif
          it = unFinishedStreams.erase(it);
      }else {
          it++;
      }
  }

  if(!finishedOperator && unFinishedStreams.size() == 0) {
    inStreamsFinished();
    finishedOperator = true; 
  }
}

/**************************
 ***** AsynchOperator *****
 **************************/

AsynchOperator::AsynchOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID): Operator(numInputs, numOutputs, ID) {}

// Loads the Operator from its serialized representation
AsynchOperator::AsynchOperator(properties::iterator props) : Operator(props) {}

// Called by an incoming Stream to communicate the given Data object
void AsynchOperator::recv(unsigned int inStreamIdx, DataPtr obj) {
//  cout << "AsynchOperator::recv(inStreamIdx="<<inStreamIdx<<") "<< endl;

  // Call the work function on the newly-arrived Data object
  work(inStreamIdx, obj);
}

// Called by Stream to indicate that the incoming stream at this index will send no more data
void AsynchOperator::streamFinished(unsigned int inStreamIdx) { 
  inStreamFinished(inStreamIdx);
}

/**************************
 ***** SourceOperator *****
 **************************/

SourceOperator::SourceOperator(unsigned int numInputs, unsigned int numOutputs, unsigned int ID): Operator(numInputs, numOutputs, ID) {}

// Loads the Operator from its serialized representation
SourceOperator::SourceOperator(properties::iterator props) : Operator(props) {}

// Called after the outputs of this Operator have been initialized. This function
// is implemented by SourceOperator and calls work(), which is implemented by derived
// classes. It also calls streamFinished() on the outgoing streams when work() exits.
void SourceOperator::driver() {
  // Call the work function of the derived class
  work();
  
  // Inform any Operators listening on the outputs of this Operator that its data has finished
  for(vector<StreamPtr>::iterator s=inStreams.begin(); s!=inStreams.end(); ++s) {
    (*s)->streamFinished();
  }
}

/**************************
 ***** InFileOperator *****
 **************************/

// inFile: points to the FILE from which we'll read data
// schema: the schema of the data from inFile
InFileOperator::InFileOperator(unsigned int ID, FILE* inFile, SchemaPtr schema): 
    SourceOperator(/*numInputs*/ 0, /*numOutputs*/ 1, ID), schema(schema), inFile(inFile) {
  assert(inFile);
  
  // This operator's user will close the given FILE
  closeFile = false;
}

// inFName: the name of the file from which we'll read data
// schema: the schema of the data from inFile
InFileOperator::InFileOperator(unsigned int ID, const char* inFName, SchemaPtr schema) :
  SourceOperator(/*numInputs*/ 0, /*numOutputs*/ 1, ID), schema(schema) {
  inFile = fopen(inFName, "r");
  assert(inFile);

  // We'll close this file
  closeFile = true;
}

// Loads the Operator from its serialized representation
InFileOperator::InFileOperator(properties::iterator props) : SourceOperator(props.next()) {
  inFile = fopen(props.get("inFName").c_str(), "r");
  assert(inFile);

  assert(props.getContents().size()==1);
  propertiesPtr schemaProps = *props.getContents().begin();
  schema = SchemaRegistry::create(schemaProps);
  assert(schema);

  // We'll close this file
  closeFile = true;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr InFileOperator::create(properties::iterator props) {
  assert(props.name()=="InFile");
  return makePtr<InFileOperator>(props);  
}

InFileOperator::~InFileOperator() {
  // Close the input file, if needed
  if(closeFile)
    fclose(inFile);
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> InFileOperator::inConnectionsComplete() {
  vector<SchemaPtr> schemas;
  schemas.push_back(schema);
  return schemas;
}

// Called to signal that all the outgoing streams have been connected. 
// Inside this call the operator may send Data objects on the outgoing streams.
// After this call the operator's work() function may be called. 
void InFileOperator::outConnectionsComplete() {}

// Called after the outputs of this Operator have been initialized.
// The function is expected to return when there is no more data to be processed.
// The implementation does not have to call streamFinished() on the outgoing streams as this
// will be done automatically by the SourceOperator base class.
void InFileOperator::work() {
  assert(outStreams.size()==1);
  
  assert(inFile);
  fgetc(inFile);
  while(!feof(inFile)) {
    fseek(inFile, -1, SEEK_CUR);
    
    // Read the next Data object on from inFile
    DataPtr data = schema->deserialize(inFile);

#ifdef VERBOSE
    char *tmp_buffer = (char *) malloc(10000);
    StreamBuffer bufferStream(tmp_buffer, 10000);
    schema->serialize(data, &bufferStream);

    printf("[InputFile]: input data stream text\n");
    int j = 0 ;
      for( j = 0 ; j < bufferStream.current_total_size ; j++){
          printf("%c",tmp_buffer[j]);
      }
    printf("\n[InputFile]:=========================================== \n\n\n");
    delete tmp_buffer;
#endif

    outStreams[0]->transfer(data);
    fgetc(inFile);
  }
    // The file has completed, inform the outgoing stream
  outStreams[0]->streamFinished();
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& InFileOperator::str(std::ostream& out) const {
  out << "[InFileOperator: ";
  Operator::str(out);
  out << "]";
  return out;
}

/********************************
 ***** InFileOperatorConfig *****
 ********************************/

InFileOperatorConfig::InFileOperatorConfig(unsigned int ID, const char* inFName, SchemaConfigPtr schemaCfg, propertiesPtr props) :
  OperatorConfig(/*numInputs*/ 0, /*numOutputs*/ 1, ID, setProperties(inFName, schemaCfg, props)) { }

propertiesPtr InFileOperatorConfig::setProperties(const char* inFName, SchemaConfigPtr schemaCfg, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["inFName"]  = inFName;  
  props->add("InFile", pMap);
  
  // Add the properties of the schema as a sub-tag of props
  if(schemaCfg->props)
    props->addSubProp(schemaCfg->props);
    
  return props;
}

/***************************
 ***** OutFileOperator *****
 ***************************/

// outFile: points to the FILE to which we'll write data
OutFileOperator::OutFileOperator(unsigned int ID, FILE* outFile): 
    AsynchOperator(/*numInputs*/ 1, /*numOutputs*/ 0, ID), outFile(outFile) {
  assert(outFile);
  
  // This operator's user will close the given FILE
  closeFile = false;
}

// outFName: the name of the file to which we'll write data
OutFileOperator::OutFileOperator(unsigned int ID, const char* outFName) :
  AsynchOperator(/*numInputs*/ 0, /*numOutputs*/ 1, ID) {
  outFile = fopen(outFName, "w");
  assert(outFile);

  // We'll close this file
  closeFile = true;
}

// Loads the Operator from its serialized representation
OutFileOperator::OutFileOperator(properties::iterator props) : AsynchOperator(props.next()) {
  outFile = fopen(props.get("outFName").c_str(), "w");
  assert(outFile);

  // We'll close this file
  closeFile = true;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr OutFileOperator::create(properties::iterator props) {
  assert(props.name()=="OutFile");
  return makePtr<OutFileOperator>(props);  
}

OutFileOperator::~OutFileOperator() {
  // Close the input file, if needed
  if(closeFile)
    fclose(outFile);
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> OutFileOperator::inConnectionsComplete() {
  vector<SchemaPtr> schemas;
  return schemas;
}

// Called when a tuple arrives on single incoming stream. 
// inStreamIdx: the index of the stream on which the object arrived
// inData: holds the single Data object from the single stream
// This function may send Data objects on some of the outgoing streams.
void OutFileOperator::work(unsigned int inStreamIdx, DataPtr inData) {
    #ifdef VERBOSE
    printf("[OUT File]: operator.... inStreamId : %d \n", inStreamIdx);
    #endif
    assert(inStreamIdx==0);
  
  //cout << "OutFileOperator::work("<<inStreamIdx<<", inData="; inData->str(cout, inStreams[0]->getSchema()); cout << endl;
  inStreams[0]->getSchema()->serialize(inData, outFile);
  fflush(outFile);

}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& OutFileOperator::str(std::ostream& out) const {
  out << "[OutFileOperator: ";
  Operator::str(out);
  out << "]";
  return out;
}

/*********************************
 ***** OutFileOperatorConfig *****
 *********************************/

OutFileOperatorConfig::OutFileOperatorConfig(unsigned int ID, const char* outFName, propertiesPtr props) :
  OperatorConfig(/*numInputs*/ 1, /*numOutputs*/ 0, ID, setProperties(outFName, props)) { }

propertiesPtr OutFileOperatorConfig::setProperties(const char* outFName, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["outFName"]  = outFName;  
  props->add("OutFile", pMap);
    
  return props;
}

/*************************************
 ***** SynchedKeyValJoinOperator *****
 *************************************/

SynchedKeyValJoinOperator::SynchedKeyValJoinOperator(unsigned int numInputs, 
                                                     unsigned int ID) : 
     SynchOperator(numInputs, /*numOutputs*/ 1, ID) {}

// Loads the Operator from its serialized representation
SynchedKeyValJoinOperator::SynchedKeyValJoinOperator(properties::iterator props): SynchOperator(props.next()) {
  assert(props.getContents().size()==0);
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr SynchedKeyValJoinOperator::create(properties::iterator props) {
  assert(props.name()=="SynchedKeyValJoin");
  return makePtr<SynchedKeyValJoinOperator>(props);  
}

void SynchedKeyValJoinOperator::inStreamsFinished() {
    if(outStreams.size() > 0){
        outStreams[0]->streamFinished();
    }
}

SynchedKeyValJoinOperator::~SynchedKeyValJoinOperator() {}
  
// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> SynchedKeyValJoinOperator::inConnectionsComplete() {
  // Grab the schema of the incoming streams and verify that all the streams use the same schema
  assert(inStreams.size()>0);
  vector<StreamPtr>::iterator in=inStreams.begin();
  for(; in!=inStreams.end(); ++in) {

    if(in==inStreams.begin()) {
      schema = dynamicPtrCast<KeyValSchema>((*in)->getSchema());
      if(!schema) { cerr << "ERROR: SynchedKeyValJoinOperator requires incoming streams to have a KeyValSchema. Actual schema is "; (*in)->getSchema()->str(cerr); cerr<<endl; assert(0); }
    } else if(schema != dynamicPtrCast<KeyValSchema>((*in)->getSchema())) {
      cerr << "ERROR: SynchedKeyValJoinOperator requires that all incoming streams use the same schema but there is an inconsistency!"<<endl;
      cerr << "Incoming stream schemas:"<<endl;
      for(int i=0; i<inStreams.size(); ++i)
      { cerr << "    "<<i<<": "; (*in)->getSchema()->str(cerr); cerr << endl; }
    }
  }

  // schema is now set to the common schema of all the input streams.
  // SynchedKeyValJoin only works with KeyValSchemas
  KeyValSchemaPtr keyValSchema = dynamicPtrCast<KeyValSchema>(schema);
  
  // Generate the schema for tuples of values from the streams
  valTupleSchema = makePtr<TupleSchema>();
  for(vector<StreamPtr>::iterator in=inStreams.begin(); in!=inStreams.end(); ++in) {
    valTupleSchema->add(keyValSchema->getValue());
  }
  
  // Generate the schema for the output of this operator
  outputKeyValSchema = makePtr<ExplicitKeyValSchema>(keyValSchema->getKey(), valTupleSchema);
  
  // Now generate the schema for the single output stream
  vector<SchemaPtr> ret;
  ret.push_back(outputKeyValSchema);
  return ret;
}

SynchedKeyValJoinOperator::joinMapFunc::joinMapFunc(SynchedKeyValJoinOperator& parent): parent(parent) {
  eKeyVal = makePtr<ExplicitKeyValMap>();
}

// Called on every key->value pair in a set of maps, 
// where the key is identical
// key - the key that is shared by the pairs in the different maps
// values - vector of all the values from the different maps
//    that have this key, one from each map
void SynchedKeyValJoinOperator::joinMapFunc::map(const DataPtr& key, 
                                                 const std::vector<DataPtr>& values) {
  // Add the current key and values to the mapping
  TuplePtr valuesTuple = makePtr<Tuple>(values, parent.valTupleSchema);
  //cout << "SynchedKeyValJoinOperator::joinMapFunc::map() parent.valTupleSchema="; parent.valTupleSchema->str(cout); cout << endl;
  //cout << "SynchedKeyValJoinOperator::joinMapFunc::map() #values="<<values.size()<<", valuesTuple="; valuesTuple->str(cout, parent.valTupleSchema); cout << endl;
  eKeyVal->add(key, valuesTuple);
}

// Called when the iteration has completed
void SynchedKeyValJoinOperator::joinMapFunc::iterComplete() {
  // Emit the mapping to the outgoing stream  
  assert(parent.outStreams.size()==1);
  
  //cout << "iterComplete() parent.outputKeyValSchema="; parent.outputKeyValSchema->str(cout); cout << endl;
  //cout << "iterComplete() eKeyVal="; eKeyVal->str(cout, parent.outputKeyValSchema); cout << endl;
  parent.outStreams[0]->transfer(eKeyVal);
}

// Called when a tuple arrives on all the incoming streams. 
// inData: holds the Data object from each stream.
// This function may send Data objects on some of the outgoing streams.
void SynchedKeyValJoinOperator::work(const std::vector<DataPtr>& inData) {
  assert(inData.size()>0);
  
  /*cout << "SynchedKeyValJoinOperator::work() inData="<<endl;
  for(vector<DataPtr>::const_iterator i=inData.begin(); i!=inData.end(); ++i)
  { cout << "    "; (*i)->str(cout, schema); cout << endl; }*/
  
  // Convert the vector of DataPtrs to a vector of KeyValMapPtrs
  vector<KeyValMapPtr> kvMaps;
  kvMaps.reserve(inData.size());
  for(vector<DataPtr>::const_iterator d=inData.begin(); d!=inData.end(); ++d)
    kvMaps.push_back(*d);
  joinMapFunc jmf(*this);
  (*kvMaps.begin())->alignMap(jmf, kvMaps, schema);
}
  
// Write a human-readable string representation of this Operator to the given output stream
std::ostream& SynchedKeyValJoinOperator::str(std::ostream& out) const {
  out << "[SynchedKeyValJoinOperator: schema="; schema->str(out); out << "]";
  return out;
}

/*******************************************
 ***** SynchedKeyValJoinOperatorConfig *****
 *******************************************/

SynchedKeyValJoinOperatorConfig::SynchedKeyValJoinOperatorConfig(
                 unsigned int numInputs, unsigned int ID,
                 propertiesPtr props) :
  OperatorConfig(numInputs, /*numOutputs*/ 1, ID, setProperties(props)) { }

propertiesPtr SynchedKeyValJoinOperatorConfig::setProperties(propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  props->add("SynchedKeyValJoin", pMap);
  
  return props;
}

/***************************
 ***** ScatterOperator *****
 ***************************/

// outFile: points to the FILE to which we'll write data
ScatterOperator::ScatterOperator(unsigned int numOutputs, unsigned int ID): 
    AsynchOperator(/*numInputs*/ 1, numOutputs, ID) {
  outStreamIdx=0;
}

// Loads the Operator from its serialized representation
ScatterOperator::ScatterOperator(properties::iterator props) : AsynchOperator(props.next()) {
  outStreamIdx=0;
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr ScatterOperator::create(properties::iterator props) {
  assert(props.name()=="Scatter");
  return makePtr<ScatterOperator>(props);  
}

ScatterOperator::~ScatterOperator() {
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> ScatterOperator::inConnectionsComplete() {
  // The schemas of all the outgoing streams are the same as the schema of the incoming one
  vector<SchemaPtr> ret;
  assert(inStreams.size()==1);
  for(int i=0; i<numOutputs; ++i)
    ret.push_back(inStreams[0]->getSchema());
  return ret;
}

// Called when a tuple arrives on single incoming stream. 
// inStreamIdx: the index of the stream on which the object arrived
// inData: holds the single Data object from the single stream
// This function may send Data objects on some of the outgoing streams.
void ScatterOperator::work(unsigned int inStreamIdx, DataPtr inData) {
  assert(outStreamIdx < outStreams.size());

  // Propagate the data object along the current outgoing stream  
  outStreams[outStreamIdx]->transfer(inData);
  
  // Advance outStreamIdx to the next output stream, wrapping around to 0 as needed
  outStreamIdx = (outStreamIdx+1)%numOutputs;
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& ScatterOperator::str(std::ostream& out) const {
  out << "[ScatterOperator: ";
  Operator::str(out);
  out << "]";
  return out;
}

/*********************************
 ***** ScatterOperatorConfig *****
 *********************************/

ScatterOperatorConfig::ScatterOperatorConfig(unsigned int numOutputs, unsigned int ID, propertiesPtr props) :
  OperatorConfig(/*numInputs*/ 1, numOutputs, ID, setProperties(props)) { }

propertiesPtr ScatterOperatorConfig::setProperties(propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  props->add("Scatter", pMap);
    
  return props;
}


/*************************************
***** RecordJoinOperator *****
*************************************/

SynchedRecordJoinOperator::SynchedRecordJoinOperator(unsigned int numInputs,
        unsigned int ID, double start, double stop, double width) :
        SynchOperator(numInputs, /*numOutputs*/ 1, ID) {
    bin_width = width;
    range_start = start;
    range_stop = stop;

}

// Loads the Operator from its serialized representation
SynchedRecordJoinOperator::SynchedRecordJoinOperator(properties::iterator props): SynchOperator(props.next()) {
    assert(props.getContents().size()==0);
    char* str_width = (char *) props.get("bin_width").c_str();
    assert(str_width);

    char* str_start = (char *) props.get("start").c_str();
    assert(str_start);

    char* str_stop = (char *) props.get("stop").c_str();
    assert(str_stop);

    //initilaize settings
    bin_width = std::stod(str_width);
    range_start = std::stod(str_start);
    range_stop = std::stod(str_stop);


}

// Creates an instance of the Operator from its serialized representation
OperatorPtr SynchedRecordJoinOperator::create(properties::iterator props) {
    assert(props.name()=="SynchedRecordJoin");
    return makePtr<SynchedRecordJoinOperator>(props);
}

void SynchedRecordJoinOperator::inStreamsFinished() {
    if(outStreams.size() > 0){
        outStreams[0]->streamFinished();
    }
}

SynchedRecordJoinOperator::~SynchedRecordJoinOperator() {}

void SynchedRecordJoinOperator::setInSchema(RecordSchemaPtr recSchmea){
    schema = recSchmea;
}

//set output Schema for this operator
void SynchedRecordJoinOperator::setOutSchema(HistogramSchemaPtr histoSchema){
    outputHistogramSchema = histoSchema;
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> SynchedRecordJoinOperator::inConnectionsComplete() {
    // Grab the schema of the incoming streams and verify that all the streams use the same schema
    assert(inStreams.size()>0);
    vector<StreamPtr>::iterator in=inStreams.begin();
    for(; in!=inStreams.end(); ++in) {

        if(in==inStreams.begin()) {
            //all incoming streams for this is record type schemas
            setInSchema(dynamicPtrCast<RecordSchema>((*in)->getSchema()));
//            schema = dynamicPtrCast<RecordSchema>((*in)->getSchema());
            if(!schema) { cerr << "ERROR: SynchedRecordJoin requires incoming streams to have a RecordSchema. Actual schema is "; (*in)->getSchema()->str(cerr); cerr<<endl; assert(0); }
        } else if(schema != dynamicPtrCast<RecordSchema>((*in)->getSchema())) {
            cerr << "ERROR: SynchedRecordJoin requires that all incoming streams use the same schema but there is an inconsistency!"<<endl;
            cerr << "Incoming stream schemas:"<<endl;
            for(int i=0; i<inStreams.size(); ++i)
            { cerr << "    "<<i<<": "; (*in)->getSchema()->str(cerr); cerr << endl; }
        }
    }

    // Generate the schema for the output of this operator
    setOutSchema(makePtr<HistogramSchema>());
//    outputHistogramSchema = makePtr<HistogramSchema>();

    // Now generate the schema for the single output stream
    vector<SchemaPtr> ret;
    ret.push_back(outputHistogramSchema);
    return ret;
}

//join all incoming records to a histogram (partial)
/*
* 1. create a histogram obj
*   1.b create bin for each respective window
* 2. for each record in the desginated range increment the count in the respective bin
* 3. add bins to histogram
*
*
* */
void SynchedRecordJoinOperator::work(const std::vector<DataPtr>& inData) {
    assert(inData.size()>0);

    //create histogram
    HistogramPtr outputHisto = makePtr<Histogram>();
    SharedPtr<Scalar<double> > min = makePtr<Scalar<double> >(range_start);
    SharedPtr<Scalar<double> > max = makePtr<Scalar<double> >(range_stop);

#ifdef VERBOSE
    cout << "[SynchedRecordJoinOperator] range start : " << range_start << " stop : " << range_stop <<   " bin width : " << bin_width << " data size : " << inData.size() << endl;
#endif
    DataPtr minDataptr = makePtr<Scalar<double> >(min->get());
    DataPtr maxDataptr = makePtr<Scalar<double> >(max->get());
    outputHisto->setMin(minDataptr) ;
    outputHisto->setMax(maxDataptr) ;

    //calculate and create number of bins
    int num_bins = (int) floor((range_stop - range_start) / bin_width);
    assert(num_bins > 0 );
    //create a map of
//    vector<HistogramBinPtr> histBins ;
//    histBins.reserve(num_bins);

    //create bins for outgoing Histogram
    for(double i = range_start ; i < range_stop ; i += bin_width){
        double bin_start = i ;
        double bin_stop ;
        if(i + bin_width >= range_stop){
            //this is the last bin
            bin_stop = range_stop;
        }else{
            bin_stop = i + bin_width;
        }
        HistogramBinPtr current_bin = makePtr<HistogramBin>();
        SharedPtr<Scalar<double> > bin_start_data = makePtr<Scalar<double> >(bin_start);
        SharedPtr<Scalar<double> > bin_stop_data = makePtr<Scalar<double> >(bin_stop);
        //initialize count with 0
        SharedPtr<Scalar<int> > bin_count_data = makePtr<Scalar<int> >(0);
        HistogramBinSchemaPtr schemaForBin = dynamicPtrCast<HistogramBinSchema>(outputHistogramSchema->value) ;

        current_bin->add(schemaForBin->field_start, bin_start_data, schemaForBin);
        current_bin->add(schemaForBin->field_end, bin_stop_data, schemaForBin);
        current_bin->add(schemaForBin->field_count, bin_count_data, schemaForBin);

        //update Histogram with Bin
        //key ==> start value ; value ==> this Histogram bin
        outputHisto->aggregateBin(bin_start_data, current_bin);
    }

    //now that we have an initialized histogram
    //do aggregate with incoming record values in 'inData'
    map<DataPtr, std::list<DataPtr> >& histodataMap = outputHisto->getDataMod();
    std::vector<DataPtr>::const_iterator dataRecordsIt = inData.begin();

    int i = 0 ;
    int j = 0 ;
    for( ; dataRecordsIt != inData.end() ; dataRecordsIt++){
        RecordPtr recs = dynamicPtrCast<Record>(*dataRecordsIt);
        vector<DataPtr>::const_iterator recIt = recs->getFields().begin();
        //for each record get the scalar data
        //update bin count
        j = 0 ;
        for(; recIt != recs->getFields().end(); recIt++){
            DataPtr p = *recIt ;
            SharedPtr<Scalar<double> > sVal_Data = dynamicPtrCast<Scalar<double> >(p);
            double sValue  = sVal_Data->get();
            //if this value is greater than some start key and less than some stop key
            //accept it to that particular bin
            // bin_i  s.t.  bin_i [E] BINS where { bin_start <= r < bin_stop }
            int binPos = (int)floor((sValue - range_start)/bin_width);

            //bin Key is the 'bin_start' value
            double binKey = range_start + (binPos)*bin_width;
            SharedPtr<Scalar<double> > binKeyData = makePtr<Scalar<double>>( binKey );

            //get the relevant bin and then update count by 1
            HistogramBinPtr binForKey = dynamicPtrCast<HistogramBin>(*histodataMap[binKeyData].begin());
            binForKey->update(1);
            i++;
            j++;
        }
    }

    #ifdef VERBOSE
    cout << "[SynchedRecordJoinOperator] histogram str ==> : " << endl       ;
    outputHisto->str(cout, outputHistogramSchema);
    #endif
    //send data upstream
    assert(outStreams.size()==1);
    outStreams[0]->transfer(outputHisto);


}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& SynchedRecordJoinOperator::str(std::ostream& out) const {
    out << "[SynchedRecordJoinOperator: schema="; schema->str(out); out << "]";
    return out;
}

/*****************************************
* SynchedRecordJoin config
*****************************************/

SynchedRecordJoinOperatorConfig::SynchedRecordJoinOperatorConfig(unsigned int numInputs, unsigned int ID,  double start,
        double stop, double width,  propertiesPtr props) :
        OperatorConfig(numInputs, /*numOutputs*/ 1, ID, setProperties(start, stop, width,
                 props)) {
}

propertiesPtr SynchedRecordJoinOperatorConfig::setProperties(  double start, double stop, double width,
        propertiesPtr props)
 {
    if (!props) props = boost::make_shared<properties>();

    map<string, string> pMap;
    pMap["start"] = to_string(start);
    pMap["stop"] = to_string(stop);
    pMap["bin_width"] = to_string(width);

    props->add("SynchedRecordJoin", pMap);

    return props;
}



/********************************
***** InMemorySourceOperator   **
********************************/

template <class NumType>
InMemorySourceOperator::RandomNumberGenerator<NumType>::RandomNumberGenerator(InMemorySourceOperator& p):parent(p){
    outData = list<DataPtr>();
}

template <class NumType>
list<DataPtr>& InMemorySourceOperator::RandomNumberGenerator<NumType>::produce(){
    outData.clear();
    //generate a random number
    srand(time(NULL));
    RecordSchemaPtr recSch = dynamicPtrCast<RecordSchema>(parent.schema);
    RecordPtr rec = makePtr<Record>(recSch);

    //we get number of numbers for this gerneration using the fields in schema
    int num = recSch->rFields.size() ;

    std::map<std::string, SchemaPtr>::const_iterator recFieldsIt = recSch->rFields.begin();
    for (int i = 0 ; i < num ; i++, recFieldsIt++) {
        NumType rand_num = parent.rnd_min + static_cast <NumType> (rand()) /( static_cast <NumType> (RAND_MAX/(parent.rnd_max - parent.rnd_min)));
        //add record to data ptr
        SharedPtr<Scalar<NumType> > rand_num_data = makePtr<Scalar<NumType>>(rand_num);

        rec->add(recFieldsIt->first, rand_num_data, parent.schema);
    }
    #ifdef VERBOSE
    printf("[InMemSourceRandomNumberGenerator]: data ready for out flow.. \n");
    rec->str(cout, parent.schema);
    printf("\n---------------- \n\n");
    #endif
    //transfer data once done
    parent.outStreams[0]->transfer(rec);

    //return data
    outData.push_back(rec);
    return outData;
}

InMemorySourceOperator::InMemorySourceOperator(unsigned int ID, unsigned int type,  int rnd_min, int rnd_max, int max_iters, SchemaPtr schema):
        SourceOperator(/*numInputs*/ 0, /*numOutputs*/ 1, ID), schema(schema){
    sourceType = type;
    this->rnd_min = rnd_min;
    this->rnd_max = rnd_max;
    this->maxIters = max_iters;
}


// Loads the Operator from its serialized representation
InMemorySourceOperator::InMemorySourceOperator(properties::iterator props) : SourceOperator(props.next()) {
    //let default be random generaator
    sourceType = RAND_SRC;
    string srcType = props.get("srcType");
    if(srcType == "RAND_SRC"){
        sourceType = RAND_SRC;
    }

    string iters = props.get("maxIterations");
    if(iters != ""){
        maxIters = stoi(iters);
    }else{
        maxIters = DEFAULT_MAX_ITERATIONS;
    }

    string strMax = props.get("rndMax");
    if(strMax != ""){
        rnd_max = stoi(strMax);
    }else{
        rnd_max = 1000 ;
    }
    string strMin = props.get("rndMin");
    if(strMax != ""){
        rnd_min = stoi(strMin);
    } else{
        rnd_min = 1 ;
    }
    assert(rnd_max >= rnd_min);

    assert(props.getContents().size()==1);
    propertiesPtr schemaProps = *props.getContents().begin();
    schema = SchemaRegistry::create(schemaProps);
    assert(schema);

}


// Creates an instance of the Operator from its serialized representation
OperatorPtr InMemorySourceOperator::create(properties::iterator props) {
    assert(props.name()=="InMemorySource");
    return makePtr<InMemorySourceOperator>(props);
}


// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> InMemorySourceOperator::inConnectionsComplete() {
    vector<SchemaPtr> schemas;
    schemas.push_back(schema);
    return schemas;
}

// Called to signal that all the outgoing streams have been connected.
// Inside this call the operator may send Data objects on the outgoing streams.
// After this call the operator's work() function may be called.
void InMemorySourceOperator::outConnectionsComplete() {}

// Called after the outputs of this Operator have been initialized.
// The function is expected to return when there is no more data to be processed.
// The implementation does not have to call streamFinished() on the outgoing streams as this
// will be done automatically by the SourceOperator base class.
void InMemorySourceOperator::work() {
    assert(outStreams.size()==1);
    #ifdef VERBOSE
    printf("[InMemSourceOperator]: start.. [iters : %d ] [ rnd_min : %d] [ rnd_max : %d] \n", maxIters, rnd_min, rnd_max);
    #endif

    int it = 0 ;
    SourceProvider* externalSource;
    while(it < maxIters) {
        if(sourceType == RAND_SRC){
            RandomNumberGenerator<double> rndGen(*this);
            externalSource = &rndGen;
        }else{
            cerr << "Invalid Source type specified [" << sourceType << "]" << endl;
            assert(0);
        }
        // transfer the next Data object from external source
        externalSource->produce();
        it++;
    }
    // The file has completed, inform the outgoing stream
    outStreams[0]->streamFinished();
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& InMemorySourceOperator::str(std::ostream& out) const {
    out << "[InMemorySourceOperator: ";
    Operator::str(out);
    out << "]";
    return out;
}

/*****************************************
* InMemorySourceOperator Config
*****************************************/

InMemorySourceOperatorConfig::InMemorySourceOperatorConfig(unsigned int ID, unsigned int type, int iters, int rnd_min, int rnd_max,
        SchemaConfigPtr schemaCfg, propertiesPtr props):
        OperatorConfig(/*numInputs*/ 0, /*numOutputs*/ 1, ID, setProperties(type, iters, rnd_min, rnd_max, schemaCfg, props)){

}

propertiesPtr InMemorySourceOperatorConfig::setProperties(unsigned int type, int iters, int rnd_min, int rnd_max,
        SchemaConfigPtr schemaCfg, propertiesPtr props){
    if(!props) props = boost::make_shared<properties>();

    //init enum strings
    map<int, string> srcType2String;
    srcType2String[InMemorySourceOperator::source_type::RAND_SRC] = "RAND_SRC";

    map<string, string> pMap;
    pMap["srcType"]  = srcType2String[type];
    pMap["maxIterations"] = to_string(iters);
    pMap["rndMax"] = to_string(rnd_max);
    pMap["rndMin"] = to_string(rnd_min);

    props->add("InMemorySource", pMap);

    // Add the properties of the schema as a sub-tag of props
    if(schemaCfg->props)
        props->addSubProp(schemaCfg->props);

    return props;

}

/**************************************
***** SynchedHistogramJoinOperator   **
**************************************/

SynchedHistogramJoinOperator::SynchedHistogramJoinOperator(unsigned int numInputs, unsigned int ID, int interval):
        SynchOperator(numInputs, /*numOutputs*/ 1, ID){
    init(interval);
}

// Loads the Operator from its serialized representation
SynchedHistogramJoinOperator::SynchedHistogramJoinOperator(properties::iterator props):SynchOperator(props.next()){
    char* str_interval = (char *) props.get("interval").c_str();
    assert(str_interval);

    //initilaize settings
    init(std::stod(str_interval));
}

// Creates an instance of the Operator from its serialized representation
OperatorPtr SynchedHistogramJoinOperator::create(properties::iterator props){
    assert(props.name()=="SynchedHistogramJoin");
    return makePtr<SynchedHistogramJoinOperator>(props);
}

void SynchedHistogramJoinOperator::init(int interval){
    synch_interval = interval;
    outputHistogram = makePtr<Histogram>();
    output_initialized = (dynamicPtrCast<Histogram>(outputHistogram))->isInitialized();
}

SynchedHistogramJoinOperator::~SynchedHistogramJoinOperator(){}

void SynchedHistogramJoinOperator::recv(unsigned int inStreamIdx, DataPtr obj){
#ifdef VERBOSE
    cout << "[SynchedHistogramJoinOperator] recv data... synch_interval : " << synch_interval << endl ;
#endif
    //lazy initialize out histogram with min/max ranges
    if(!output_initialized){
        HistogramPtr outHistogram = dynamicPtrCast<Histogram>(outputHistogram);
        HistogramPtr curr_h = dynamicPtrCast<Histogram>(obj);
        outHistogram->setMin(curr_h->getMin());
        outHistogram->setMax(curr_h->getMax());

        //reset checker
        output_initialized = outHistogram->isInitialized();
    }

    //push incoming data to buffer
    dataBuffer.push_back(obj);

    //check if number of incoming data objects exceed 'synch_interval'
    if(dataBuffer.size() == synch_interval){
        work(dataBuffer);
        dataBuffer.clear();
    }
}

// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> SynchedHistogramJoinOperator::inConnectionsComplete(){
    assert(inStreams.size()>0);
    vector<StreamPtr>::iterator in=inStreams.begin();
    for(; in!=inStreams.end(); ++in) {

        if(in==inStreams.begin()) {
            //all incoming streams for this is record type schemas
            schema = dynamicPtrCast<HistogramSchema>((*in)->getSchema());
            if(!schema) { cerr << "ERROR: SynchedHistogramJoin requires incoming streams to have a HistogramSchema. Actual schema is "; (*in)->getSchema()->str(cerr); cerr<<endl; assert(0); }
        } else if(schema != dynamicPtrCast<HistogramSchema>((*in)->getSchema())) {
            cerr << "ERROR: SynchedHistogramJoin requires that all incoming streams use the same schema but there is an inconsistency!"<<endl;
            cerr << "Incoming stream schemas:"<<endl;
            for(int i=0; i<inStreams.size(); ++i)
            { cerr << "    "<<i<<": "; (*in)->getSchema()->str(cerr); cerr << endl; }
        }
    }
    vector<SchemaPtr> ret;
    ret.push_back(schema);
    return ret;
}


void SynchedHistogramJoinOperator::work(const std::vector<DataPtr>& inData){
    HistogramPtr outHistogram = dynamicPtrCast<Histogram>(outputHistogram);
    //start aggregating data
    std::vector<DataPtr>::const_iterator bufferIt = inData.begin();
    for(; bufferIt != inData.end() ; bufferIt++){
        HistogramPtr curr_h = dynamicPtrCast<Histogram>(*bufferIt);
        outHistogram->join(curr_h);
    }

#ifdef VERBOSE
    cout << "[SynchedHistogramJoinOperator] work() joined schema : " << endl ;
    //outHistogram->str(cout, schema);
#endif
}

void SynchedHistogramJoinOperator::inStreamsFinished(){
    cout << "[SynchedHistogramJoinOperator] streams waiting for sucessfull completion. records left to join :  " << dataBuffer.size()  << endl ;
    //check if any histograms left in buffer
    if(dataBuffer.size() > 0){
        //if we have anything left join them
        HistogramPtr outHistogram = dynamicPtrCast<Histogram>(outputHistogram);
        //start aggregating data
        std::vector<DataPtr>::const_iterator bufferIt = dataBuffer.begin();
        for(; bufferIt != dataBuffer.end() ; bufferIt++){
            HistogramPtr curr_h = dynamicPtrCast<Histogram>(*bufferIt);
            outHistogram->join(curr_h);
        }
#ifdef VERBOSE
        cout << "[SynchedHistogramJoinOperator][inStreamsFinished] joined schema : " << endl ;
        outHistogram->str(cout, schema);
#endif

    }
    //do data transfer operation
    if(outStreams.size() > 0){
    	outStreams[0]->transfer(outputHistogram);
        outStreams[0]->streamFinished();
    }
}

// Write a human-readable string representation of this Operator to the given output stream
std::ostream& SynchedHistogramJoinOperator::str(std::ostream& out) const {
    out << "[SynchedHistogramJoinOperator: schema="; schema->str(out); out << "]";
    return out;
}

/*****************************************
* SynchedHistogramJoinOperator Config
*****************************************/

SynchedHistogramJoinOperatorConfig::SynchedHistogramJoinOperatorConfig(unsigned int ID, int interval, propertiesPtr props):
        OperatorConfig(/*numInputs*/ 1, /*numOutputs*/ 1, ID, setProperties(interval, props)){

}

propertiesPtr SynchedHistogramJoinOperatorConfig::setProperties(int interval, propertiesPtr props){
    if(!props) props = boost::make_shared<properties>();


    map<string, string> pMap;
    pMap["interval"]  = std::to_string(interval);

    props->add("SynchedHistogramJoin", pMap);

    return props;

}

