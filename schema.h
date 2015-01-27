#pragma once
#include "sight_common_internal.h"
#include "comp_shared_ptr.h"
#include "data.h"
#include <vector>
#include <map>
#include <boost/enable_shared_from_this.hpp>

using namespace sight;

/* Base class for schemas that describe the types of data items that operators
 *   may consume and produce on each stream. Schemas help to check for type compatibility
 *   and to plan valid and efficient execution plans for continuous queries. Schemas
 *   include information about the structure of the data (e.g. matrix, graph, int) 
 *   as well as notes that describe constraints on possible data values (e.g. 
 *   monotonically increasing or block-cyclic partitioned). 
 * Schemas are organized around containment relations and do not represent inheritance 
 *   information for individual compound types in the scheme (e.g. if we have an vector 
 *   of ints and a vector inherits from array, the schema is vector(int) and there is no 
 *   mention of array).
 * Since Schemas have all the organization for Data objects, they are also generators 
 *   for such objects.
 */
class Schema;
typedef SharedPtr<Schema> SchemaPtr;

#ifdef SCHEMA_C
SchemaPtr NULLSchemaPtr;
#else
extern SchemaPtr NULLSchemaPtr;
#endif

class SchemaConfig;
typedef SharedPtr<SchemaConfig> SchemaConfigPtr;

/**
* A circular streaming buffer for the stream operators
*  adaptive - change size of buffer depending on the current total
*/
class StreamBuffer {
    public:
    //holding buffer for the incoming/outgoing stream
    const void* buffer;
    //total size of current stream
    int current_total_size;
    //current maximum size allowed on the stream
    int max_size;

    //end position
    int seek ;
    //start position of the sttream
    int start ;

    StreamBuffer(const void* out, int max_size){
        this->buffer = out ;
        //init sizes
        this->max_size = max_size;
        this->current_total_size = 0 ;
        //init start/end to first pos
        seek = 0 ;
        start = 0 ;
    }

    ~StreamBuffer(){
        delete (char*)buffer;
    }

} ;

class Schema {
  public:
  Schema() {}
  Schema(properties::iterator props) {}
  	
  // Return whether this object is identical to that object
  virtual bool operator==(const SchemaPtr& that) const=0;
  bool operator!=(const SchemaPtr& that) const { return !((*this) == that); }
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  virtual bool operator<(const SchemaPtr& that) const=0;
  bool operator<=(const SchemaPtr& that) const { return (*this) == that || (*this) < that; }
  bool operator> (const SchemaPtr& that) const { return !((*this) < that); }
  bool operator>=(const SchemaPtr& that) const { return (*this) == that || (*this) > that; }
  
  // Serializes the given data object into and writes it to the given outgoing stream
  virtual void serialize(DataPtr obj, FILE* out) const=0;


    /*  each time a data portion is serialized check if currently 'bytes_written' > size/2
    *   - if true resize buffer to size*2
    *
    *  memcopy 'data' to updated (or not) buffer allocation 'buffer'
    *
    *  return - number of bytes written
    *  */
  static int bufwrite(const void* data, int size, StreamBuffer * buffer);

  static int bufread(void* data, int size, StreamBuffer * buffer);

  static int bufgetc(StreamBuffer * buffer);


    //Serializes the given data object into and writes it to a given buffer
  virtual void serialize(DataPtr obj, StreamBuffer * buffer) const=0;
  
  // Reads the serialized representation of a Data object from the stream, 
  // creates a binary representation of the object and returns a shared pointer to it.
  virtual DataPtr deserialize(FILE* in) const=0;

  virtual DataPtr deserialize(StreamBuffer * in) const=0;
  	
  // Write a human-readable string representation of this object to the given
  // output stream
  virtual std::ostream& str(std::ostream& out) const=0;
    
  // Returns the Schema configuration object that describes this schema. Such configurations
  // can be created without creating a full schema (more expensive) but if we already have
  // a schema, this method makes it possible to get its configuration.
  virtual SchemaConfigPtr getConfig() const=0;

/*  // Maps unique names of schema types to pointers to their respective Schema objects
  static std::map<std::string, SchemaPtr> schemas;

  // Registers a given schema with the global schemas map
  static void registerSchema(const std::string& name, SchemaPtr schema);

  // Reads a new Data object from the given stream
  static DataPtr readData(FILE* f)*/
}; // class Schema


// Mapping of unique schema names to functions that create SchemaPtrs from their serialized representation
class SchemaRegistry {
  public:
  typedef SchemaPtr (*creator)(properties::iterator props);
  static std::map<std::string, creator> creators;
  
  // Maps the given label to a creator function, returning whether this mapping overrides a prior one (true) or is
  // a fresh mapping (false).	
  static bool regCreator(const std::string& label, creator create);
  
  static SchemaPtr create(propertiesPtr props);
}; // SchemaRegistry

// Schemas need to be serialized and deserialized. The structure of Schemas is managed by SchemaConfig objects. 
//   For each class that derives from Schema 
//   there should be a corresponding Config class that derives from SchemaConfig. Each constructor of
//   a Config class takes a pointer to a properties object as an argument, which defaults to NULL.
//   The constructor then calls a static setProperties() method that is implements, which  takes 
//   this pointer as an argument. setProperties() adds the key->value pairs specific to its class to
//   the given properties object, creating a fresh one if it got a NULL pointer. It then returns a pointer
//   to the properties object, which is propagated as an argument to the parent class constructor.
//   When the process reaches the constructor for the base SchemaConfig class we have a fully formed
//   properties object that has separate entries for all the classes up the inheritance stack, ordered from
//   the most derived to base class. This properties object can then be serialized and its serial
//   representation can be deserialized to create fresh Schema objects.
class SchemaConfig {
  public:
  propertiesPtr props;
  
  SchemaConfig(propertiesPtr props=NULLProperties) : props(props) {}
}; // SchemaConfig


/*****************
 ***** Tuple *****
 *****************/

// Schema for tuples that contain data items of arbitrary type
class TupleSchemaConfig;
class TupleSchema: public Schema, public boost::enable_shared_from_this<TupleSchema> {
  friend class TupleSchemaConfig;
  
  public:
  // The schemas of this tuple's fields
  std::vector<SchemaPtr> tFields;
  
  // Loads the TupleSchema from a configuration file.
  TupleSchema(properties::iterator props);
  
  // Creates an instance of the schema from its serialized representation
  static SchemaPtr create(properties::iterator props);
  
  // Creates an uninitialized TupleSchema. add() must be called to set it up.
  TupleSchema();
  
  // Creates a TupleSchema with a fixed tuple of DataPtrs.
  TupleSchema(const std::vector<SchemaPtr> &tFields);
  
  public:
  // Adds the given schema after all the schemas that have already been added.
  void add(SchemaPtr schema);
  
  // Adds the given schema at the given index within the schema. Returns whether a schema was previously
  // mapped at this index
  bool add(unsigned int idx, SchemaPtr schema);
  
  // Returns a shared pointer to the Schema currently mapped to the given index
  SchemaPtr get(unsigned int idx) const;
    
  const std::vector<SchemaPtr>& getFields() const { return tFields; }
  	
  // Return whether this object is identical to that object
  bool operator==(const SchemaPtr& that_arg) const;
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const SchemaPtr& that_arg) const;

  // Serializes the given data object into and writes it to the given outgoing stream
  void serialize(DataPtr obj, FILE* out) const;

  // Serializes the given data object into and writes it to the given outgoing stream buffer
  void serialize(DataPtr obj, StreamBuffer * buffer) const;
  
  // Reads the serialized representation of a Data object from the stream, 
  // creates a binary representation of the object and returns a shared pointer to it.
  DataPtr deserialize(FILE* in) const;

  DataPtr deserialize(StreamBuffer * in) const;
  	
  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out) const;
    
  // Returns the Schema configuration object that describes this schema. Such configurations
  // can be created without creating a full schema (more expensive) but if we already have
  // a schema, this method makes it possible to get its configuration.
  SchemaConfigPtr getConfig() const;
}; // class TupleSchema
typedef SharedPtr<TupleSchema> TupleSchemaPtr;
typedef SharedPtr<const TupleSchema> ConstTupleSchemaPtr;

class TupleSchemaConfig: public SchemaConfig {
  public:
  TupleSchemaConfig(const std::vector<SchemaConfigPtr> &tFields, propertiesPtr props=NULLProperties);
    
  propertiesPtr setProperties(const std::vector<SchemaConfigPtr> &tFields, propertiesPtr props);
}; // class TupleSchemaConfig
typedef SharedPtr<TupleSchemaConfig> TupleSchemaConfigPtr;

/******************
 ***** Record *****
 ******************/

// Schema for named records, which maps string names to DataPtr values
class RecordSchemaConfig;
class RecordSchema: public Schema, public boost::enable_shared_from_this<RecordSchema> {
  friend class RecordSchemaConfig;
  
  public:
  // Maps the names of this record's fields to the schemas of their types
  std::map<std::string, SchemaPtr> rFields;
    
  // Maps the names of this record's fields to their unique indexes in the rFields
  // vector maintained by RecordData instances of this RecordSchema
  std::map<std::string, unsigned int> field2Idx;
  
  // Records whether this schema's structure has been finalized (i.e. nothing else may be added) or not
  bool schemaFinalized;
  
  // Loads the RecordSchema from a configuration file. add() or finalize() may not be called after this constructor.
  RecordSchema(properties::iterator props);
  
  // Creates an instance of the schema from its serialized representation
  static SchemaPtr create(properties::iterator props);
  
  // Creates an uninitialized RecordSchema. add() must be called to set it up and finalize() to complete the mapping.
  RecordSchema();
  
  // Creates a RecordSchema with a fixed mapping of labels to DataPtrs. add() or finalize() may not be called after this constructor.
  RecordSchema(const std::map<std::string, SchemaPtr> &rFields);
  
  public:
  // Maps the given field name to the given schema, returning whether this mapping overrides a prior one (true) or is
  // a fresh mapping (false).
  bool add(const std::string& label, SchemaPtr schema);
  
  // Finalizes the label->DataPtr mapping of this record so that no more fields may be added.
  void finalize();
  
  // Returns a shared pointer to the Schema currently mapped to the given field name.
  SchemaPtr get(const std::string& label) const;
    
  // Returns the unique index of this label in the rFields
  // vector maintained by RecordData instances of this RecordSchema
  unsigned int getIdx(const std::string& label) const;
  
  const std::map<std::string, SchemaPtr>& getFields() const { return rFields; }
  	
  // Return whether this object is identical to that object
  bool operator==(const SchemaPtr& that_arg) const;
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const SchemaPtr& that_arg) const;

  // Serializes the given data object into and writes it to the given outgoing stream
  void serialize(DataPtr obj, FILE* out) const;

  // Serializes the given data object into and writes it to the given outgoing stream buffer
  void serialize(DataPtr obj, StreamBuffer * buffer) const;
  
  // Reads the serialized representation of a Data object from the stream, 
  // creates a binary representation of the object and returns a shared pointer to it.
  DataPtr deserialize(FILE* in) const;

  DataPtr deserialize(StreamBuffer * in) const;

    // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out) const;
    
  // Returns the Schema configuration object that describes this schema. Such configurations
  // can be created without creating a full schema (more expensive) but if we already have
  // a schema, this method makes it possible to get its configuration.
  SchemaConfigPtr getConfig() const;
}; // class RecordSchema
typedef SharedPtr<RecordSchema> RecordSchemaPtr;
typedef SharedPtr<const RecordSchema> ConstRecordSchemaPtr;

class RecordSchemaConfig: public SchemaConfig {
  public:
  RecordSchemaConfig(const std::map<std::string, SchemaConfigPtr> &rFields, propertiesPtr props=NULLProperties);
    
  propertiesPtr setProperties(const std::map<std::string, SchemaConfigPtr> &rFields, propertiesPtr props);
}; // class RecordSchemaConfig
typedef SharedPtr<RecordSchemaConfig> RecordSchemaConfigPtr;

/******************
 ***** KeyVal *****
 ******************/

// Data objects are opaque, except for the few Operators specifically designed to
// understand their semantics and APIs. To make it possible to design generic operators
// that work with a wide range of objects we employ Schemas that are primarily focused 
// on providing standard interfaces to the information inside Data objects without 
// specifying anything about their internal details.

// The KeyValSchema represents the idea of Key->Value maps. These include Records, Dense and 
// Sparse matrixes, hashtables and graphs (edge->value, node->value). The KeyValSchema
// allows Operators to iterate over the key->value mapping and to perform joins 
// The KeyValSchema provides the structure for accessing the KeyValMap Data object, which
// encodes the standard API for accesing objects as Key->Value maps.
class KeyValSchema: public Schema, public boost::enable_shared_from_this<KeyValSchema> {
  public:
  SchemaPtr key;
  SchemaPtr value;
  
  KeyValSchema(const SchemaPtr& key, const SchemaPtr& value);
  
  // Loads the Schema from a configuration file. add() or finalize() may not be called after this constructor.
  KeyValSchema(properties::iterator props);
  
  const SchemaPtr& getKey()   const { return key; }
  const SchemaPtr& getValue() const { return value; }
  
  // Return whether this object is identical to that object
  bool operator==(const SchemaPtr& that_arg) const;
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const SchemaPtr& that_arg) const;
  
  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out) const;
}; // class KeyValSchema
typedef SharedPtr<KeyValSchema> KeyValSchemaPtr;
typedef SharedPtr<const KeyValSchema> ConstKeyValSchemaPtr;

class KeyValSchemaConfig: public SchemaConfig {
  public:
  KeyValSchemaConfig(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props=NULLProperties);
    
  propertiesPtr setProperties(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props);
}; // class KeyValSchemaConfig

/**************************
 ***** ExplicitKeyVal *****
 **************************/

// Schema for an explicit representation of key->value mappings (ExplicitKeyValMap) 
// that keeps it as a list of key->value pairs.
class ExplicitKeyValSchema : public KeyValSchema {
  public:
  ExplicitKeyValSchema(const SchemaPtr& key, const SchemaPtr& value) : 
  	KeyValSchema(key, value) {}
  
  // Loads the Schema from a configuration file. add() or finalize() may not be called after this constructor.
  ExplicitKeyValSchema(properties::iterator props);
  
  // Creates an instance of the schema from its serialized representation
  static SchemaPtr create(properties::iterator props);
  
  // Serializes the given data object into and writes it to the given outgoing stream
  void serialize(DataPtr obj, FILE* out) const;

  // Serializes the given data object into and writes it to the given outgoing stream buffer
  void serialize(DataPtr obj, StreamBuffer * buffer) const;

  // Reads the serialized representation of a Data object from the stream, 
  // creates a binary representation of the object and returns a shared pointer to it.
  DataPtr deserialize(FILE* in) const;

  DataPtr deserialize(StreamBuffer * in) const;

    // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out) const;
    
  // Returns the Schema configuration object that describes this schema. Such configurations
  // can be created without creating a full schema (more expensive) but if we already have
  // a schema, this method makes it possible to get its configuration.
  SchemaConfigPtr getConfig() const;
}; // class ExplicitKeyValSchema
typedef SharedPtr<ExplicitKeyValSchema> ExplicitKeyValSchemaPtr;
typedef SharedPtr<const ExplicitKeyValSchema> ConstExplicitKeyValSchemaPtr;

class ExplicitKeyValSchemaConfig: public KeyValSchemaConfig {
  public:
  ExplicitKeyValSchemaConfig(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props=NULLProperties);
    
  propertiesPtr setProperties(const SchemaConfigPtr& key, const SchemaConfigPtr& value, propertiesPtr props);
}; // class ExplicitKeyValSchemaConfig

/******************
 ***** Scalar *****
 ******************/

// Schemas of scalars of various base types
class ScalarSchemaConfig;
class ScalarSchema: public Schema, public boost::enable_shared_from_this<ScalarSchema> {
  friend class ScalarSchemaConfig;
  
  public:
  typedef enum {charT, stringT, intT, longT, floatT, doubleT} scalarType;
  private:
  scalarType type;
  public:
  ScalarSchema(scalarType type);
  ScalarSchema(properties::iterator props);
    
  // Creates an instance of the schema from its serialized representation
  static SchemaPtr create(properties::iterator props);

  // Return whether this object is identical to that object
  bool operator==(const SchemaPtr& that_arg) const;
  
  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const SchemaPtr& that_arg) const;
  
  // Creates an instance of the schema from its serialized representation
//  SchemaPtr create(properties::iterator props);

  // Serializes the given data object into and writes it to the given outgoing stream
  void serialize(DataPtr obj, FILE* out) const;

// Serializes the given data object into and writes it to the given outgoing stream buffer
  //returns size of the data written into buffer
  void serialize(DataPtr obj, StreamBuffer * buffer) const;

    // Reads the serialized representation of a Data object from the stream,
  // creates a binary representation of the object and returns a shared pointer to it.
  DataPtr deserialize(FILE* in) const;

  DataPtr deserialize(StreamBuffer * in) const;


    // Returns this Schema's scalar type
  scalarType getType() const { return type; }

  // Returns a string representation of this schema's type
  static std::string type2Str(scalarType type);
  
  // String representation of this Schema's scalar type
  std::string type2Str() const { return type2Str(getType()); }

  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out) const;
    
  // Returns the Schema configuration object that describes this schema. Such configurations
  // can be created without creating a full schema (more expensive) but if we already have
  // a schema, this method makes it possible to get its configuration.
  SchemaConfigPtr getConfig() const;
}; // class ScalarSchema
typedef SharedPtr<ScalarSchema> ScalarSchemaPtr;

class ScalarSchemaConfig: public SchemaConfig {
  public:
  ScalarSchemaConfig(ScalarSchema::scalarType type, propertiesPtr props=NULLProperties);
    
  propertiesPtr setProperties(ScalarSchema::scalarType type, propertiesPtr props);
}; // class ScalarSchemaConfig

/*
// Implementation of an n-dimensional dense array KeyValMap, where the keys are n-dim 
// numeric Records and the values are arbitrary Records
class nDimDenseArraySchema : public KeyValSchema {
  // The dimensionality of the array
  typedef easyvector<int> dims;
  
  // The dimensionality of the overall space spanned by this data source
  nDimDenseArraySchema ::dims spaceDims;

  // The schema of the values in this array
  SchemaPtr valSchema;
  public:
  nDimDenseArraySchema(dims spaceDims, SchemaPtr valSchema): 
            spaceDims(spaceDims), valSchema(valSchema) {}

  // Return whether this object is identical to that object
  virtual bool operator==(const Schema& that_arg) {
    try {
      const nDimDenseArraySchema& that = dynamic_cast<const nDimDenseArraySchema&>(that_arg);
      if(d.size() != that.d.size()) return false;
      
      for(vector<int>::const_iterator itThis = d.begin(), itThat=that.d.begin();
             itThat!=d.end(); itThis++, itThat++) {
        if(*itThis != *itThat) return false;
      }
      if(valSchema!=that.valSchema) return false;
      return true;
    } catch (std::bad_cast& bc)
    { return false; }
  }

  nDimDenseArrayPtr getInstance(nDimDenseArraySchema ::dims spaceDims) 
  { return boost::make_shared<nDimDenseArraySchema>(spaceDims); }
}; // class nDimDenseArraySchema
typedef SharedPtr<nDimDenseArraySchema> nDimDenseArraySchemaPtr;

*/