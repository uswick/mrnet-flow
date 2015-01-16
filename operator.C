#include "operator.h"
#include <limits.h>
#include <stdio.h>
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

std::map<std::string, OperatorRegistry::creator> OperatorRegistry::creators;

// Maps the given label to a creator function, returning whether this mapping overrides a prior one (true) or is
// a fresh mapping (false).	
bool OperatorRegistry::regCreator(const std::string& label, OperatorRegistry::creator create) {
  std::map<std::string, creator>::iterator i=creators.find(label);
  creators.insert(make_pair(label, create));
  return i!=creators.end();
}

OperatorPtr OperatorRegistry::create(propertiesPtr props) {
	if(creators.find(props->name()) == creators.end()) { cerr << "ERROR: no deserializer available for "<<props->name()<<"!"<<endl; assert(0); }
  return creators[props->name()](props->begin());
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
  if(!finishedOperator) {
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
  assert(inStreamIdx==0);
  
  //cout << "OutFileOperator::work("<<inStreamIdx<<", inData="; inData->str(cout, inStreams[0]->getSchema()); cout << endl;
  inStreams[0]->getSchema()->serialize(inData, outFile);
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

SynchedKeyValJoinOperator::~SynchedKeyValJoinOperator() {}
  
// Called to signal that all the incoming streams have been connected. Returns the schemas
// of the outgoing streams based on the schemas of the incoming streams.
std::vector<SchemaPtr> SynchedKeyValJoinOperator::inConnectionsComplete() {
  // Grab the schema of the incoming streams and verify that all the streams use the same schema
  assert(inStreams.size()>0);
  for(vector<StreamPtr>::iterator in=inStreams.begin(); in!=inStreams.end(); ++in) {
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




