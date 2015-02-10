#define SCHEMA_C
#include "schema.h"
#include <boost/exception/detail/type_info.hpp>
#include <assert.h>
#include <stdio.h>
#include <vector>
#include <unistd.h>

using namespace std;

/******************
 ***** Schema *****
 ******************/

/*
// Maps unique names of schema types to pointers to their respective Schema objects
std::map<std::string, SchemaPtr> Schema::schemas;

// Registers a given schema with the global schemas map
void Schema::registerSchema(const std::string& name, SchemaPtr schema) {
  schemas[name] = schema;
}

// Reads a new Data object from the given stream
DataPtr Schema::readData(FILE* f) {
  // First read the name of the schema that will deserialize the data
  char schemaName[1000];
  int i=0;
  do {
    schemaName[i] = fgetc(f);
  } while(schemaName[i] != '\0');
  
  // Next read the size of the blob
  uint_32 size;
  fread(f, &size, sizeof(size));

  // Finally, read the blob's data into memory
  char* data = new char[size];
  fgets(f, data, size);

  map<string, SchemaPtr>::iterator schema = schemas.find(string(schemaName));
  if(schema == schemas.end()) {
    cerr << "ERROR: unknown schema "<<schemaName<<endl;
    assert(0);
  }
  
  return schema->second->deserialize(size, data);
}
*/

/*  each time a data portion is serialized check if currently 'write_total' > max_size/2
*   - if true resize buffer to size*2
*
*  memcopy 'data' to updated (or not) buffer allocation 'buffer'
*
*  return - number of bytes written
*  we use a circular buffer
*
*             <---curr total---->
*     <----------- max size --------->
*     |  |  |  |  |  |  |  |  |  |  |
*
*             ^start            ^seek
*  */

int Schema::bufwrite(const void* data, int data_size, StreamBuffer * buffer){
    int write_total =  buffer->current_total_size + data_size;
//    printf("Schema::bufwrite [start] data_size %d current_total_size %d  write_total : %d \n",
//            data_size, buffer->current_total_size, write_total);

    //reallocate logic
    if(write_total > (buffer->max_size)){
        //need to resize
        realloc((void*)buffer->buffer, buffer->max_size * 2);
        buffer->max_size = buffer->max_size *2 ;
    }

    //if we have enough space left until end to write the data
    if((buffer->seek + data_size) <= buffer->max_size){
        //memcopy the current data portion into buffer
        memcpy(((char*)buffer->buffer + buffer->seek), data, data_size);
    }else{
        int bytes_spill = (buffer->seek + data_size) % buffer->max_size;
        int bytes_to_end = data_size - bytes_spill;
        //copy first half
        memcpy(((char*)buffer->buffer + buffer->seek), data, bytes_to_end);
        //copy second half
        memcpy((char*)buffer->buffer, ((char*)data + bytes_to_end), bytes_spill);
    }
    //update current size
    buffer->current_total_size += data_size;
    //update seek position
    buffer->seek = (buffer->seek + data_size) % buffer->max_size;

//    printf("Schema::bufwrite [before end] data_size %d current_total_size %d  write_total : %d max_size allowed : %d seek %d start %d \n",
//            data_size, buffer->current_total_size, write_total, buffer->max_size, buffer->seek, buffer->start);
    return buffer->current_total_size ;
}

/**
* copy data from buffer to input buffer 'size' number of bytes
*
* return - 1 if success
* return - (-1) if not success, either error or not enough sized data ('size') available in StreamBuffer.
*               Buffer position is not progressed
*
* we use a circular buffer
*
*             <---curr total---->
*     <----------- max size --------->
*     |  |  |  |  |  |  |  |  |  |  |
*
*             ^start            ^seek
*/
int Schema::bufread(void* input_buf, int size, StreamBuffer * buffer){
    //check if we have enough characters available in buffer as requested to copy
    if(buffer->current_total_size >= size){
        int i = 0 ;
        for(i = 0 ; i < size ; i++){
            int pos = (buffer->start + i) % buffer->max_size;
            *((char*)input_buf + i) = *((char*)buffer->buffer + pos);
        }
        buffer->start = (buffer->start + size) % buffer->max_size;
        //update total size
        buffer->current_total_size -= size;
        return 1;
    }
    //we don't have enough data available for this operation
    //return error
    return -1;
}

int Schema::bufgetc(StreamBuffer * buffer){
    char c = '\0';
    int ret = bufread(&c , 1, buffer);

    if(ret == -1)
        return ret;

    return c;
}


/**************************
 ***** SchemaRegistry *****
 **************************/

//std::map<std::string, SchemaRegistry::creator> SchemaRegistry::creators;

boost::thread_specific_ptr< std::map<std::string, SchemaRegistry::creator> > SchemaRegistry::creatorsInstance;

// Maps the given label to a creator function, returning whether this mapping overrides a prior one (true) or is
// a fresh mapping (false).	
bool SchemaRegistry::regCreator(const std::string& label, SchemaRegistry::creator create) {
//  std::map<std::string, creator>::iterator i=creators.find(label);
//  creators.insert(make_pair(label, create));
//  return i!=creators.end();

  std::map<std::string, creator>::iterator i=(*getCreators()).find(label);
  (*getCreators()).insert(make_pair(label, create));
  return i!=(*getCreators()).end();

}

SchemaPtr SchemaRegistry::create(propertiesPtr props) {
//	if(creators.find(props->name()) == creators.end()) { cerr << "ERROR: no deserializer available for "<<props->name()<<"!"<<endl; assert(0); }
//  return creators[props->name()](props->begin());

    if((*getCreators()).find(props->name()) == (*getCreators()).end()) { cerr << "ERROR: no deserializer available for "<<props->name()<<"!"<<endl; assert(0); }
    return (*getCreators())[props->name()](props->begin());
}

/************************
 ***** TupleSchema *****
 ************************/

// Loads the TupleSchema from a configuration file. 
TupleSchema::TupleSchema(properties::iterator props) : Schema(props.next()) {
  assert(props.name()=="Tuple");
  
  int numFields = props.getInt("numFields");
  
  assert(numFields == props.getContents().size());
  
  int i=0;
  for(list<propertiesPtr>::const_iterator sub=props.getContents().begin(); sub!=props.getContents().end(); ++sub, ++i) {
    SchemaPtr valSchema = SchemaRegistry::create(*sub);
    add(valSchema);
  }
}

// Creates an instance of the schema from its serialized representation
SchemaPtr TupleSchema::create(properties::iterator props) {
  assert(props.name()=="Tuple");
  return makePtr<TupleSchema>(props);
}

// Creates an uninitialized TupleSchema. add() must be called to set it up.
TupleSchema::TupleSchema() {
}

// Creates a TupleSchema with a fixed mapping of labels to DataPtrs. 
TupleSchema::TupleSchema(const std::vector<SchemaPtr> &tFields): tFields(tFields) {
}

// Adds the given schema after all the schemas that have already been added.
void TupleSchema::add(SchemaPtr schema)
{ tFields.push_back(schema); }

// Adds the given schema at the given index within the schema. Returns whether a schema was previously
// mapped at this index
bool TupleSchema::add(unsigned int idx, SchemaPtr schema) {
  tFields.reserve(idx+1);
  bool previouslyMapped = (tFields[idx].get()!=NULL);
  tFields[idx] = schema;
  return previouslyMapped;
}

// Returns a shared pointer to the Schema currently mapped to the given index
SchemaPtr TupleSchema::get(unsigned int idx) const {
  assert(tFields.size()>idx);
  return tFields[idx];
}

// Return whether this object is identical to that object
bool TupleSchema::operator==(const SchemaPtr& that_arg) const {
  TupleSchemaPtr that = dynamicPtrCast<TupleSchema>(that_arg);
  if(!that) { cerr << "ERROR: TupleSchema::operator== is provided incompatible object "<<that_arg->str(cerr)<<"!"; }
    
  if(tFields.size() != that->tFields.size()) return false;
    
  for(std::vector<SchemaPtr>::const_iterator itThis = tFields.begin(), itThat=that->tFields.begin();
      itThat!=tFields.end(); itThis++, itThat++) {
    if(*itThis  != *itThat || !(*itThis == *itThat))
      return false;
  }
  return true;
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool TupleSchema::operator<(const SchemaPtr& that_arg) const {
  TupleSchemaPtr that = dynamicPtrCast<TupleSchema>(that_arg);
  if(!that) { cerr << "ERROR: TupleSchema::operator== is provided incompatible object "<<that_arg->str(cerr)<<"!"; }
  
  if(tFields.size() < that->tFields.size()) return true;
  if(tFields.size() < that->tFields.size()) return false;
  
  std::vector<SchemaPtr>::const_iterator itThis = tFields.begin(), itThat=that->tFields.begin();
  for(; itThis!=tFields.end() && itThat!=that->tFields.end(); itThis++, itThat++) {
    if(*itThis  < *itThat) return true;
    if(*itThis  > *itThat) return false;
  }
  // If all the tFields this and that have in common are equal, compare based on length, with the shorter Tuple 
  // ordered before the longer
  if(itThis!=tFields.end() && itThat!=that->tFields.end()) assert(0);
  if(itThis==tFields.end() && itThat==that->tFields.end()) return false; // they're equal
  if(itThis==tFields.end() && itThat!=that->tFields.end()) return true;
  if(itThis!=tFields.end() && itThat==that->tFields.end()) return false;
  assert(0);
}

// Serializes the given data object into and writes it to the given outgoing stream
void TupleSchema::serialize(DataPtr obj_arg, FILE* out) const {
  TuplePtr obj = dynamicPtrCast<Tuple>(obj_arg);
  if(!obj) { cerr << "ERROR: TupleSchema::serialize is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
  
  //cout << "#tFields="<<tFields.size()<<", #obj->tFields="<<obj->tFields.size()<<endl;
  assert(tFields.size() == obj->tFields.size());
  vector<SchemaPtr>::const_iterator sField=tFields.begin();
  vector<DataPtr>::const_iterator dField=obj->tFields.begin();
  for(; sField!=tFields.end(); ++sField, ++dField) {
    (*sField)->serialize(*dField, out);
  }
}

// Serializes the given data object into and writes it to the given outgoing buffer stream
void TupleSchema::serialize(DataPtr obj_arg, StreamBuffer * out) const {
    TuplePtr obj = dynamicPtrCast<Tuple>(obj_arg);
    if(!obj) { cerr << "ERROR: TupleSchema::serialize is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }

    //cout << "#tFields="<<tFields.size()<<", #obj->tFields="<<obj->tFields.size()<<endl;
    assert(tFields.size() == obj->tFields.size());
    vector<SchemaPtr>::const_iterator sField=tFields.begin();
    vector<DataPtr>::const_iterator dField=obj->tFields.begin();
    for(; sField!=tFields.end(); ++sField, ++dField) {
        (*sField)->serialize(*dField, out);
    }
}

// Reads the serialized representation of a Data object from the stream, 
// creates a binary representation of the object and returns a shared pointer to it.
DataPtr TupleSchema::deserialize(FILE* in) const {
  TuplePtr rec = makePtr<Tuple>(shared_from_this());

  // Read each field
//  cout << "TupleSchema::deserialize() #rFields="<<rFields.size()<<endl;
  for(vector<SchemaPtr>::const_iterator sField=tFields.begin(); sField!=tFields.end(); ++sField) {
//    cout << "    "<<sField->first<<endl;
    DataPtr fieldD = (*sField)->deserialize(in);
//    cout << "    adding"<<endl;
    rec->add(fieldD, shared_from_this());
  }

  return rec;
}

DataPtr TupleSchema::deserialize(StreamBuffer * in) const {
    TuplePtr rec = makePtr<Tuple>(shared_from_this());

    // Read each field
//  cout << "TupleSchema::deserialize() #rFields="<<rFields.size()<<endl;
    for(vector<SchemaPtr>::const_iterator sField=tFields.begin(); sField!=tFields.end(); ++sField) {
//    cout << "    "<<sField->first<<endl;
        DataPtr fieldD = (*sField)->deserialize(in);
//    cout << "    adding"<<endl;
        rec->add(fieldD, shared_from_this());
    }

    return rec;
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& TupleSchema::str(std::ostream& out) const {
  out << "[TupleSchema: "<<endl;
  int i=0;
  for(vector<SchemaPtr>::const_iterator f=tFields.begin(); f!=tFields.end(); ++f, ++i) {
    if(f!=tFields.begin()) out << endl;
    out << "    "<<i<<": "; (*f)->str(out);
  }
  out << "]";
  return out;
}

// Returns the Schema configuration object that describes this schema. Such configurations
// can be created without creating a full schema (more expensive) but if we already have
// a schema, this method makes it possible to get its configuration.
SchemaConfigPtr TupleSchema::getConfig() const {
  vector<SchemaConfigPtr> tFieldsConfig;
  for(vector<SchemaPtr>::const_iterator f=tFields.begin(); f!=tFields.end(); ++f)
    tFieldsConfig.push_back((*f)->getConfig());
  return makePtr<TupleSchemaConfig>(tFieldsConfig);
}

/*****************************
 ***** TupleSchemaConfig *****
 *****************************/
TupleSchemaConfig::TupleSchemaConfig(const std::vector<SchemaConfigPtr> &tFields, propertiesPtr props) :
  SchemaConfig(setProperties(tFields, props)) { }

propertiesPtr TupleSchemaConfig::setProperties(const std::vector<SchemaConfigPtr> &tFields, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["numFields"] = txt()<<tFields.size();
  
  int i=0;
  for(vector<SchemaConfigPtr>::const_iterator f=tFields.begin(); f!=tFields.end(); ++f, ++i) {
    assert((*f)->props);
    // Add the Configuration of each field as a sub-tag of props
    props->addSubProp((*f)->props);
  }
  props->add("Tuple", pMap);
  
  return props;
}

/************************
 ***** RecordSchema *****
 ************************/

// Loads the RecordSchema from a configuration file. add() or finalize() may not be called after this constructor.
RecordSchema::RecordSchema(properties::iterator props) : Schema(props.next()) {
  assert(props.name()=="Record");

  schemaFinalized = false;
  
  int numFields = props.getInt("numFields");
  
  assert(numFields == props.getContents().size());
  
  int i=0;
  for(list<propertiesPtr>::const_iterator sub=props.getContents().begin(); sub!=props.getContents().end(); ++sub, ++i) {
    SchemaPtr valSchema = SchemaRegistry::create(*sub);
    add(props.get(txt()<<"field_"<<i), valSchema);
  }

  finalize();
}

// Creates an instance of the schema from its serialized representation
SchemaPtr RecordSchema::create(properties::iterator props) {
  assert(props.name()=="Record");
  return makePtr<RecordSchema>(props);
}

// Creates an uninitialized RecordSchema. add() must be called to set it up and finalize() to complete the mapping.
RecordSchema::RecordSchema() {
  schemaFinalized = false;
}

// Creates a RecordSchema with a fixed mapping of labels to DataPtrs. add() or finalize() may not be called after this constructor
RecordSchema::RecordSchema(const std::map<std::string, SchemaPtr> &rFields): rFields(rFields) {
  finalize();
}

// Maps the given field name to the given schema, returning whether this mapping overrides a prior one (true) or is
// a fresh mapping (false).
bool RecordSchema::add(const std::string& label, SchemaPtr schema) {
  assert(!schemaFinalized);
  map<std::string, SchemaPtr>::iterator i=rFields.find(label);
  rFields.insert(make_pair(label, schema));
  return i!=rFields.end();
}

// Finalizes the label->DataPtr mapping of this record so that no more fields may be added.
void RecordSchema::finalize() {
  // Set up the mapping from field labels to their unique indexes in the rFields
  // vector maintained by RecordData instances of this RecordSchema
  unsigned int i=0;
  for(map<string, SchemaPtr>::iterator f=rFields.begin(); f!=rFields.end(); ++f, ++i) {
    field2Idx[f->first] = i;
  }
    

  schemaFinalized = true;
}

// Returns a shared pointer to the Schema currently mapped to the given field name.
SchemaPtr RecordSchema::get(const std::string& label) const {
  std::map<std::string, SchemaPtr>::const_iterator f=rFields.find(label);
  if(f == rFields.end()) return NULLSchemaPtr;
  else return f->second;
}

// Returns the unique index of this label in the rFields
// vector maintained by RecordData instances of this RecordSchema
unsigned int RecordSchema::getIdx(const std::string& label) const {
  assert(schemaFinalized);

//  printf("Record Schema : lbl : %s \n", label.c_str());
  /*cout << "#field2Idx="<<field2Idx.size()<<endl;
  for(map<std::string, unsigned int>::const_iterator f=field2Idx.begin(); f!=field2Idx.end(); ++f)
    cout << "    "<<f->first<<": "<<f->second<<endl;*/
  
  map<string, unsigned int>::const_iterator i=field2Idx.find(label);
  assert(i!=field2Idx.end());
  
  return i->second;
}
  
// Return whether this object is identical to that object
bool RecordSchema::operator==(const SchemaPtr& that_arg) const {
  RecordSchemaPtr that = dynamicPtrCast<RecordSchema>(that_arg);
  if(!that) { cerr << "ERROR: RecordSchema::operator== is provided incompatible object "<<that_arg->str(cerr)<<"!"; }
  
  if(rFields.size() != that->rFields.size()) return false;
  
  for(std::map<std::string, SchemaPtr>::const_iterator itThis = rFields.begin(), itThat=that->rFields.begin();
      itThat!=rFields.end(); itThis++, itThat++) {
    if(itThis->first  != itThat->first ||
       !(itThis->second == itThat->second)) 
      return false;
  }
  return true;
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool RecordSchema::operator<(const SchemaPtr& that_arg) const {
  RecordSchemaPtr that = dynamicPtrCast<RecordSchema>(that_arg);
  if(!that) { cerr << "ERROR: RecordSchema::operator== is provided incompatible object "<<that_arg->str(cerr)<<"!"; }
    
  if(rFields.size() < that->rFields.size()) return true;
  if(rFields.size() < that->rFields.size()) return false;
  
  std::map<std::string, SchemaPtr>::const_iterator itThis = rFields.begin(), itThat=that->rFields.begin();
  for(; itThis!=rFields.end() && itThat!=that->rFields.end(); itThis++, itThat++) {
    if(itThis->first  < itThat->first) return true;
    if(itThis->first  > itThat->first) return false;
    if(itThis->second < itThat->second) return true;
    if(itThis->second > itThat->second) return false;
  }
  // If all the rFields this and that have in common are equal, compare based on length, with the shorter Record 
  // ordered before the longer
  if(itThis!=rFields.end() && itThat!=that->rFields.end()) assert(0);
  if(itThis==rFields.end() && itThat==that->rFields.end()) return false; // they're equal
  if(itThis==rFields.end() && itThat!=that->rFields.end()) return true;
  if(itThis!=rFields.end() && itThat==that->rFields.end()) return false;
  assert(0);
}

// Serializes the given data object into and writes it to the given outgoing stream
void RecordSchema::serialize(DataPtr obj_arg, FILE* out) const {
    RecordPtr obj = dynamicPtrCast<Record>(obj_arg);
    if(!obj) { cerr << "ERROR: RecordSchema::serialize is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }

    //cout << "#rFields="<<rFields.size()<<", #obj->rFields="<<obj->rFields.size()<<endl;
    assert(rFields.size() == obj->rFields.size());
    map<string, SchemaPtr>::const_iterator sField=rFields.begin();
    vector<DataPtr>::const_iterator dField=obj->rFields.begin();
    for(; sField!=rFields.end(); ++sField, ++dField) {
        sField->second->serialize(*dField, out);
    }
}

void RecordSchema::serialize(DataPtr obj_arg, StreamBuffer * out) const {
    RecordPtr obj = dynamicPtrCast<Record>(obj_arg);
    if(!obj) { cerr << "ERROR: RecordSchema::serialize is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }

    //cout << "#rFields="<<rFields.size()<<", #obj->rFields="<<obj->rFields.size()<<endl;
    assert(rFields.size() == obj->rFields.size());
    map<string, SchemaPtr>::const_iterator sField=rFields.begin();
    vector<DataPtr>::const_iterator dField=obj->rFields.begin();
    for(; sField!=rFields.end(); ++sField, ++dField) {
        sField->second->serialize(*dField, out);
    }
}

// Reads the serialized representation of a Data object from the stream, 
// creates a binary representation of the object and returns a shared pointer to it.
DataPtr RecordSchema::deserialize(FILE* in) const {
  RecordPtr rec = makePtr<Record>(shared_from_this());

  // Read each field
//  cout << "RecordSchema::deserialize() #rFields="<<rFields.size()<<endl;
  for(map<string, SchemaPtr>::const_iterator sField=rFields.begin(); sField!=rFields.end(); ++sField) {
//    cout << "    "<<sField->first<<endl;
    DataPtr fieldD = sField->second->deserialize(in);
//    cout << "    adding"<<endl;
    rec->add(sField->first, fieldD, shared_from_this());
  }

  return rec;
}

DataPtr RecordSchema::deserialize(StreamBuffer * in) const {
    RecordPtr rec = makePtr<Record>(shared_from_this());

    // Read each field
//  cout << "RecordSchema::deserialize() #rFields="<<rFields.size()<<endl;
    for(map<string, SchemaPtr>::const_iterator sField=rFields.begin(); sField!=rFields.end(); ++sField) {
//    cout << "    "<<sField->first<<endl;
        DataPtr fieldD = sField->second->deserialize(in);
//    cout << "    adding"<<endl;
        rec->add(sField->first, fieldD, shared_from_this());
    }

    return rec;
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& RecordSchema::str(std::ostream& out) const {
  out << "[RecordSchema: "<<endl;
  for(map<string, SchemaPtr>::const_iterator f=rFields.begin(); f!=rFields.end(); ++f) {
    if(f!=rFields.begin()) out << endl;
    out << "    "<<f->first<<": "; f->second->str(out);
  }
  out << "]";
  return out;
}

// Returns the Schema configuration object that describes this schema. Such configurations
// can be created without creating a full schema (more expensive) but if we already have
// a schema, this method makes it possible to get its configuration.
SchemaConfigPtr RecordSchema::getConfig() const {
  std::map<string, SchemaConfigPtr> rFieldsConfig;
  for(map<string, SchemaPtr>::const_iterator r=rFields.begin(); r!=rFields.end(); ++r)
    rFieldsConfig[r->first] = r->second->getConfig();
  return makePtr<RecordSchemaConfig>(rFieldsConfig);
}

/******************************
 ***** RecordSchemaConfig *****
 ******************************/
RecordSchemaConfig::RecordSchemaConfig(const std::map<std::string, SchemaConfigPtr> &rFields, propertiesPtr props) :
  SchemaConfig(setProperties(rFields, props)) { }

propertiesPtr RecordSchemaConfig::setProperties(const std::map<std::string, SchemaConfigPtr> &rFields, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["numFields"] = txt()<<rFields.size();
  
  int i=0;
  for(map<string, SchemaConfigPtr>::const_iterator f=rFields.begin(); f!=rFields.end(); ++f, ++i) {
    assert(f->second->props);
    // Record the name of each field
    pMap[txt()<<"field_"<<i] = f->first;
    
    // Add the Configuration of each field as a sub-tag of props
    props->addSubProp(f->second->props);
  }
  props->add("Record", pMap);
  
  return props;
}

/************************
 ***** KeyValSchema *****
 ************************/
 
KeyValSchema::KeyValSchema(const SchemaPtr& key, const SchemaPtr& value) :
  key(key), value(value) {}

// Loads the RecordSchema from a configuration file. add() or finalize() may not be called after this constructor.
KeyValSchema::KeyValSchema(properties::iterator props) {
  assert(props.name()=="KeyVal");
  assert(props.getContents().size() == 2);
  
  list<propertiesPtr>::const_iterator keyIt = props.getContents().begin();
  list<propertiesPtr>::const_iterator valIt = keyIt; ++valIt;
  
  key   = SchemaRegistry::create(*keyIt);
  value = SchemaRegistry::create(*valIt);
}

// Return whether this object is identical to that object
bool KeyValSchema::operator==(const SchemaPtr& that_arg) const {
  try {
    KeyValSchemaPtr that = dynamicPtrCast<KeyValSchema>(that_arg);
    return key==that->key && value==that->value;
  } catch (std::bad_cast& bc)
  { return false; }
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool KeyValSchema::operator<(const SchemaPtr& that_arg) const {
  try {
    KeyValSchemaPtr that = dynamicPtrCast<KeyValSchema>(that_arg);
    return  key< that->key ||
           (key==that->key && value<that->value);
  } catch (std::bad_cast& bc) {
    // For different schema types user pointer comparison
    return this < that_arg.get();
  }
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& KeyValSchema::str(std::ostream& out) const {
  out << "[KeyValSchema: "<<endl;
  out << "    key=";   key->str(out);   out << endl;
  out << "    value="; value->str(out); out << "]";
  return out;
}

/******************************
 ***** KeyValSchemaConfig *****
 ******************************/
KeyValSchemaConfig::KeyValSchemaConfig(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props) :
  SchemaConfig(setProperties(key, value, props)) { }

propertiesPtr KeyValSchemaConfig::setProperties(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  props->add("KeyVal", pMap);

  // Add the Configuration of the key and value as a sub-tag of props
  props->addSubProp(key->props);
  props->addSubProp(value->props);
  
  return props;
}

/********************************
 ***** ExplicitKeyValSchema *****
 ********************************/

ExplicitKeyValSchema::ExplicitKeyValSchema(properties::iterator props) : KeyValSchema(props.next()) {
  // There is nothing to do since the ExplicitKeyValSchema doesn't add any additional 
  // state on top of the KeyValSchema
}

// Creates an instance of the schema from its serialized representation
SchemaPtr ExplicitKeyValSchema::create(properties::iterator props) {
  assert(props.name()=="ExplicitKeyVal");
  return makePtr<ExplicitKeyValSchema>(props);
}

// Serializes the given data object into and writes it to the given outgoing stream
void ExplicitKeyValSchema::serialize(DataPtr obj_arg, FILE* out) const {
  ExplicitKeyValMapPtr obj = dynamicPtrCast<ExplicitKeyValMap>(obj_arg);
  if(!obj) { cerr << "ERROR: ExplicitKeyValSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
  
  // Write out the number of key mappings we'll emit
  unsigned int numKeys = obj->getData().size();
  fwrite(&numKeys, sizeof(unsigned int), 1, out);
  
  // Iterate through each key->value mapping in obj
  for(std::map<DataPtr, std::list<DataPtr> >::const_iterator i=obj->getData().begin(); i!=obj->getData().end(); i++) {
    // Serialize the current key
    key->serialize(i->first, out);
    
    // Serialize all the values mapped to this key
    
    // First, the number of values
    unsigned int numValues = i->second.size();
    fwrite(&numValues, sizeof(unsigned int), 1, out);
    
    // The the values themselves
    for(std::list<DataPtr>::const_iterator j=i->second.begin(); j!=i->second.end(); j++) {
      //cout << "valueSchema="; value->str(cout); cout << endl;
      value->serialize(*j, out);
    }
  }
}

/**
* out - output buffer to write to
* size - max num bytes allowed
*
*/

void ExplicitKeyValSchema::serialize(DataPtr obj_arg, StreamBuffer * out) const {
    ExplicitKeyValMapPtr obj = dynamicPtrCast<ExplicitKeyValMap>(obj_arg);
    if(!obj) { cerr << "ERROR: ExplicitKeyValSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }

    int bytes_written = 0 ;
    // Write out the number of key mappings we'll emit
    unsigned int numKeys = obj->getData().size();
//    fwrite(&numKeys, sizeof(unsigned int), 1, out);
    int total_written = bufwrite(&numKeys,sizeof(unsigned int), out);
//    printf("Schema::ExplicitKeyValSchema bufwrite numkeys... total_written : %d \n", total_written);

    // Iterate through each key->value mapping in obj
    for(std::map<DataPtr, std::list<DataPtr> >::const_iterator i=obj->getData().begin(); i!=obj->getData().end(); i++) {
        // Serialize the current key
        key->serialize(i->first, out);

        // Serialize all the values mapped to this key

        // First, the number of values
        unsigned int numValues = i->second.size();
//        fwrite(&numValues, sizeof(unsigned int), 1, out);
        bufwrite(&numValues,sizeof(unsigned int), out);

        // The the values themselves
        for(std::list<DataPtr>::const_iterator j=i->second.begin(); j!=i->second.end(); j++) {
            //cout << "valueSchema="; value->str(cout); cout << endl;
            value->serialize(*j, out);
        }
    }
}


// Reads the serialized representation of a Data object from the stream, 
// creates a binary representation of the object and returns a shared pointer to it.
DataPtr ExplicitKeyValSchema::deserialize(FILE* in) const {
  ExplicitKeyValMapPtr kvMap = makePtr<ExplicitKeyValMap>();
  map<DataPtr, list<DataPtr> >& data = kvMap->getDataMod();
  
  // Read the number of keys
  unsigned int numKeys;
  fread(&numKeys, sizeof(unsigned int), 1, in);
  
  // Load that number of Keys
  for(unsigned int k=0; k<numKeys; ++k) {
    // Load the key itself
    DataPtr keyD = key->deserialize(in);

    pair<map<DataPtr, list<DataPtr> >::iterator, bool> keyLoc = data.insert(make_pair(keyD, list<DataPtr>()));
    
    // Read the number of values mapped to this key
    unsigned int numValues;
    fread(&numValues, sizeof(unsigned int), 1, in);
    
    // Load this number of values and map them to the key
    for(unsigned int v=0; v<numValues; ++v) {
      DataPtr valueD = value->deserialize(in);
      
      keyLoc.first->second.push_back(valueD);
    }
  }
  
  return kvMap;
}


DataPtr ExplicitKeyValSchema::deserialize(StreamBuffer * in) const {
//    printf("Schema::ExplicitKeyValSchema[deserialize] start ... PID : %d\n", getpid());

    ExplicitKeyValMapPtr kvMap = makePtr<ExplicitKeyValMap>();
    map<DataPtr, list<DataPtr> >& data = kvMap->getDataMod();

    int ret ;
    // Read the number of keys
    unsigned int numKeys;

//    printf("Schema::ExplicitKeyValSchema[deserialize] bufread numkeys...PID : %d thread ID: %d \n",  getpid(), pthread_self());
    ret = bufread(&numKeys, sizeof(unsigned int), in);
//    printf("Schema::ExplicitKeyValSchema[deserialize] bufread numkeys done.. keys : %d  ret : %d ...PID : %d\n", numKeys, ret,  getpid());
//    fflush(stdout);

    if(ret == -1) return NULLData;

    // Load that number of Keys
    for(unsigned int k=0; k<numKeys; ++k) {
        // Load the key itself
        DataPtr keyD = key->deserialize(in);

        pair<map<DataPtr, list<DataPtr> >::iterator, bool> keyLoc = data.insert(make_pair(keyD, list<DataPtr>()));

        // Read the number of values mapped to this key
        unsigned int numValues;
        ret = bufread(&numValues, sizeof(unsigned int), in);
//        printf("Schema::ExplicitKeyValSchema[deserialize] bufread Values : %d return : %d ... PID : %d thread ID: %d  \n", numValues, ret, getpid(), pthread_self());
//        fflush(stdout);

        if(ret == -1) return NULLData;

        // Load this number of values and map them to the key
        for(unsigned int v=0; v<numValues; ++v) {
            DataPtr valueD = value->deserialize(in);

            keyLoc.first->second.push_back(valueD);
        }
    }

    return kvMap;
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& ExplicitKeyValSchema::str(std::ostream& out) const {
  out << "[ExplicitKeyValSchema: "; KeyValSchema::str(out); "]";
  return out;
}

// Returns the Schema configuration object that describes this schema. Such configurations
// can be created without creating a full schema (more expensive) but if we already have
// a schema, this method makes it possible to get its configuration.
SchemaConfigPtr ExplicitKeyValSchema::getConfig() const {
  return makePtr<ExplicitKeyValSchemaConfig>(key->getConfig(), value->getConfig());
}

/**************************************
 ***** ExplicitKeyValSchemaConfig *****
 **************************************/
ExplicitKeyValSchemaConfig::ExplicitKeyValSchemaConfig(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props) :
  KeyValSchemaConfig(key, value, setProperties(key, value, props)) { }

propertiesPtr ExplicitKeyValSchemaConfig::setProperties(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  props->add("ExplicitKeyVal", pMap);

  return props;
}

/************************
 ***** ScalarSchema *****
 ************************/

ScalarSchema::ScalarSchema(scalarType type): type(type) {}

/*ScalarSchema::ScalarSchema(properties::iterator props) {
  assert(props.name()=="Scalar");
  typeName = props.get("typeName");  
}*/

// Loads the Schema from a configuration file. add() or finalize() may not be called after this constructor.
ScalarSchema::ScalarSchema(properties::iterator props) : Schema(props.next()) {
  assert(props.name()=="Scalar");
  type = (scalarType)props.getInt("type");
}

// Creates an instance of the schema from its serialized representation
SchemaPtr ScalarSchema::create(properties::iterator props) {
  assert(props.name()=="Scalar");
  return makePtr<ScalarSchema>(props);
}

// Return whether this object is identical to that object
bool ScalarSchema::operator==(const SchemaPtr& that_arg) const { 
  try {
    ScalarSchemaPtr that = dynamicPtrCast<ScalarSchema>(that_arg);
    //return typeName == that.typeName;
    return type == that->type;
  } catch (std::bad_cast& bc)
  { return false; }
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool ScalarSchema::operator<(const SchemaPtr& that_arg) const {
  try {
    ScalarSchemaPtr that = dynamicPtrCast<ScalarSchema>(that_arg);
    //return typeName < that->typeName;
    return type < that->type;
  } catch (std::bad_cast& bc) { 
    // For different schema types use pointer inequality
    return this < that_arg.get();
  }
}

/* // Creates an instance of the schema from its serialized representation
SchemaPtr ScalarSchema::create(properties::iterator props) {
  assert(props.name()=="Scalar");
  return makePtr<ScalarSchema>(props);
}*/

// Serializes the given data object into and writes it to the given outgoing stream
void ScalarSchema::serialize(DataPtr obj_arg, FILE* out) const {
  switch(type) {
    case charT: {
      SharedPtr<Scalar<char> > obj = SharedPtr<Scalar<char> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      fwrite(&obj->get(), sizeof(char), 1, out);
      break; }

    case stringT: {
      SharedPtr<Scalar<string> > obj = SharedPtr<Scalar<string> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      // Write the string and its terminating NUL character
      fwrite(obj->get().c_str(), sizeof(char)*(obj->get().size()+1), 1, out);
      break; }

    case intT: {
      SharedPtr<Scalar<int> > obj = dynamicPtrCast<Scalar<int> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      fwrite(&obj->get(), sizeof(int), 1, out);
      break; }

    case longT: {
      SharedPtr<Scalar<long> > obj = SharedPtr<Scalar<long> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      fwrite(&obj->get(), sizeof(long), 1, out);
      break; }
      
    case floatT: {
      SharedPtr<Scalar<float> > obj = dynamicPtrCast<Scalar<float> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      fwrite(&obj->get(), sizeof(float), 1, out);
      break; }
      
    case doubleT: {
      SharedPtr<Scalar<double> > obj = SharedPtr<Scalar<double> >(obj_arg);
      if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
      fwrite(&obj->get(), sizeof(double), 1, out);
      break; }
      
    default:
      assert(0);
  }  
}


void ScalarSchema::serialize(DataPtr obj_arg, StreamBuffer * out) const {
    switch(type) {
        case charT: {
            SharedPtr<Scalar<char> > obj = SharedPtr<Scalar<char> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            bufwrite(&obj->get(), sizeof(char), out);
            break; }

        case stringT: {
            SharedPtr<Scalar<string> > obj = SharedPtr<Scalar<string> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            // Write the string and its terminating NUL character
            bufwrite(obj->get().c_str(), sizeof(char)*(obj->get().size()+1), out);
            break; }

        case intT: {
            SharedPtr<Scalar<int> > obj = dynamicPtrCast<Scalar<int> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            bufwrite(&obj->get(), sizeof(int), out);
            break; }

        case longT: {
            SharedPtr<Scalar<long> > obj = SharedPtr<Scalar<long> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            bufwrite(&obj->get(), sizeof(long), out);
            break; }

        case floatT: {
            SharedPtr<Scalar<float> > obj = dynamicPtrCast<Scalar<float> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            bufwrite(&obj->get(), sizeof(float), out);
            break; }

        case doubleT: {
            SharedPtr<Scalar<double> > obj = SharedPtr<Scalar<double> >(obj_arg);
            if(!obj) { cerr << "ERROR: ScalarSchema::serialize() is provided incompatible object "<<obj_arg->str(cerr, shared_from_this())<<"!"; assert(0); }
            bufwrite(&obj->get(), sizeof(double), out);
            break; }

        default:
            assert(0);
    }
}
  
// Reads the serialized representation of a Data object from the stream, 
// creates a binary representation of the object and returns a shared pointer to it.
DataPtr ScalarSchema::deserialize(FILE* in) const {
//  cout << "ScalarSchema::deserialize() feof(in)="<<feof(in)<<", "; str(cout); cout << endl;
  
  switch(type) {
    case charT: {
      SharedPtr<Scalar<char> > newS = makePtr<Scalar<char> >();
      fread(&newS->getMod(), sizeof(char), 1, in);
      return newS;
    }

    case stringT: {
      char data[100000];
      // Read from in until we find the first NULL character
      unsigned int i=0;
      while((data[i] = fgetc(in)) != '\0') { ++i; }
      return makePtr<Scalar<string> >(string(data));
    }

    case intT: {
      int data;
      fread(&data, sizeof(int), 1, in);
      return makePtr<Scalar<int> >(data);
    }

    case longT: {
      long data;
      fread(&data, sizeof(long), 1, in);
      return makePtr<Scalar<long> >(data);
    }

    case floatT: {
      float data;
      fread(&data, sizeof(float), 1, in);
      return makePtr<Scalar<float> >(data);
    }

    case doubleT: {
      double data;
      fread(&data, sizeof(double), 1, in);
  
      return makePtr<Scalar<double> >(data);
    }
    
    default: assert(0);
  }
}


DataPtr ScalarSchema::deserialize(StreamBuffer * in) const {
//  cout << "ScalarSchema::deserialize() feof(in)="<<feof(in)<<", "; str(cout); cout << endl;
    int ret ;
    switch(type) {
        case charT: {
            SharedPtr<Scalar<char> > newS = makePtr<Scalar<char> >();
            ret = bufread(&newS->getMod(), sizeof(char), in);
            if(ret == -1) return NULLData;
            return newS;
        }

        case stringT: {
            char data[100000];
            // Read from in until we find the first NULL character
            unsigned int i=0;
            while(true) {
                    ret = bufgetc(in) ;
                    if(ret == -1){
                        return NULLData;
                    }else{
                        data[i] = ret;
                        if(data[i] == '\0' ){
                            break;
                        }
                    }
                    ++i;
            }
            return makePtr<Scalar<string> >(string(data));
        }

        case intT: {
            int data;
            ret = bufread(&data, sizeof(int), in);
            if(ret == -1) return NULLData;

            return makePtr<Scalar<int> >(data);
        }

        case longT: {
            long data;
            ret = bufread(&data, sizeof(long), in);
            if(ret == -1) return NULLData;

            return makePtr<Scalar<long> >(data);
        }

        case floatT: {
            float data;
            ret = bufread(&data, sizeof(float), in);
            if(ret == -1) return NULLData;
            return makePtr<Scalar<float> >(data);
        }

        case doubleT: {
            double data;
            ret = bufread(&data, sizeof(double), in);
            if(ret == -1) return NULLData;
            return makePtr<Scalar<double> >(data);
        }

        default: assert(0);
    }
}

// Returns a string representation of this schema's type
std::string ScalarSchema::type2Str(scalarType type) {
  //cout << "ScalarSchema::type2Str("<<type<<")"<<endl;
  switch(type) {
    case charT:   return "char";
    case stringT: return "string";
    case intT:    return "int";
    case longT:   return "long";
    case floatT:  return "float";
    case doubleT: return "double";
    default: assert(0);
  }
  cout <<"????"<<endl;
}
  
// Write a human-readable string representation of this object to the given
// output stream
std::ostream& ScalarSchema::str(std::ostream& out) const {
  out << "[Scalar: "<<type2Str(type)<<"]";  
  return out;
}

// Returns the Schema configuration object that describes this schema. Such configurations
// can be created without creating a full schema (more expensive) but if we already have
// a schema, this method makes it possible to get its configuration.
SchemaConfigPtr ScalarSchema::getConfig() const {
  return makePtr<ScalarSchemaConfig>(type);
}

/******************************
 ***** ScalarSchemaConfig *****
 ******************************/
ScalarSchemaConfig::ScalarSchemaConfig(ScalarSchema::scalarType type, propertiesPtr props) :
  SchemaConfig(setProperties(type, props)) { }

propertiesPtr ScalarSchemaConfig::setProperties(ScalarSchema::scalarType type, propertiesPtr props) {
  if(!props) props = boost::make_shared<properties>();
  
  map<string, string> pMap;
  pMap["type"] = txt()<<type;
  props->add("Scalar", pMap);
  
  return props;
}
