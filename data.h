#pragma once
#include "sight_common_internal.h"
#include "comp_shared_ptr.h"
#include "schema.h"
#include <vector>
#include <map>
#include <assert.h>
#include <typeinfo>
#include <iostream>
using namespace std;

class Schema;
typedef SharedPtr<Schema> SchemaPtr;
typedef SharedPtr<const Schema> ConstSchemaPtr;

/*******************************
 ***** Abstract Interfaces *****
 *******************************/
class Data;
typedef SharedPtr<Data> DataPtr;
class Data {
  public:
  // ----- Comparison Methods, useful for constructing -----
  // ----- log-time data structures of Data objects    -----
  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  virtual bool operator==(const DataPtr& that) const=0;
  bool operator!=(const DataPtr& that) const { return !((*this) == that); }

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  virtual bool operator<(const DataPtr& that) const=0;
  bool operator<=(const DataPtr& that) const { return (*this) == that || (*this) < that; }
  bool operator> (const DataPtr& that) const { return !((*this) < that); }
  bool operator>=(const DataPtr& that) const { return (*this) == that || (*this) > that; }

  // ----- Unique Hashkey Methods, useful for constructing -----
  // ----- near-linear time data structures of Data        -----
  // ----- objects (optional).                             -----
  // Returns a unique number that identifies this object
  virtual long uniqHashKey() { return 0; }
  virtual bool implementsUniqHashKey() { return false; }
  
  // ----- Distance Methods, useful for clustering sets of -----
  // ----- Data objects.                                   -----
  // Return the distance (measure of dis-similarity) between this object and the given object.
  // that must have a type that is compatible with this
  virtual bool distance(const Data& that) { return 0; } 
  virtual bool implementsDistance() { return false; }

  // Call the parent class's getName call and then Append this class' unique name to the name list.
  void getName(std::list<std::string>& name) const { name.push_back("Data"); }
  
  std::list<std::string> getName() const {
    std::list<std::string> name;
    getName(name);
    return name;
  }
  
  // Write a human-readable string representation of this object to the given
  // output stream
  virtual std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const=0;

/*  // ----- Methods for creating unique string descriptions -----
  // ----- of Data objects that account for their          -----
  // ----- inheritance hierarchy.                          -----
  // Derived classes must call macro DATA_DEFS(className, parentClass), where
  // className - the name of the class itself
  // parentClass - the name of the class it directly derives from
  
  // The following macros create methods:
  //    - getName(): returns the string name of the current class 
  //    - getType(): returns a list that encodes this object's unique hierarchical structure (signature of its C++ type)
  // Derived classes should not implement getName() or getType() on their own. They are generated automatically
  // by teh DATA_DEFS macro
  #define DATA_GET_NAME(className) \
   /* Returns the unique name of this derived class * /
   virtual std::string getName() const { return symbol(className); }
  /* Implements the getType method by calling the method in the parent * /
  /* class and adding the name of this class * /
  std::list<std::string> getType() { 
    std::list<std::string> type;
    getType_rec(type);
    return type;
  }

  #define DATA_GET_TYPE(className, parentClass)      \
    /* Recursive body of getType() that appends strings to a list declared inside getType() * / \
    void getType_rec(std::list<std::string>& type) { \
      parentClass::getType_rec(type);                \
      type.push_back(getName());                     \
    }

  // Implementation of getType_rec in the Data class that forms the base case of the recursion
  void getType_rec(std::list<std::string> & type) { }

  // ----- Methods for serializing Data items. Each derived class XYZ implements method  -----
  // ----- void serializeXYZ(properties*) that takes a non-NULL pointer to a properties object, adds    -----
  // ----- the key->value pairs that describe it to that object and returns it. Then, to serialize an object one  -----
  // ----- must call object.serialize() and the return value is a newly-allocated properties      -----
  // ----- object that contains the encoded representation.                                                                           -----

  // Given a serialized representation of the this object, as reported by the classes that this class
  // inherits from, adds key->value pairs that represent this class and returns the resulting object
  #define DATA_SERIALIZE(className, parentClass)                    \
    /* Implements the serialize() method by calling the method in the parent class and then the current class * / \
    properties* serialize() {                                       \
      return serialize ## className(parentClass::serialize()); \
    }
    
  // Base-case implementation of serialization, which creates an empty
  // properties object
  properties* serialize() {
    assert(props==NULL);
    return new properties();
  }

  #define DATA_DEFS(className, parentClass) \
    DATA_GET_NAME(className)                \
    DATA_GET_TYPE(className, parentClass)   \
    DATA_SERIALIZE(className, parentClass)
*/
}; // class Data

/*bool operator==(DataPtr a, DataPtr b)  { return *(a.get()) == b; }
bool operator!=(DataPtr a, DataPtr b)  { return *(a.get()) != b; }
bool operator< (DataPtr a, DataPtr b)  { return *(a.get()) <  b; }
bool operator<=(DataPtr a, DataPtr b)  { return *(a.get()) <= b; }
bool operator> (DataPtr a, DataPtr b)  { return *(a.get()) >  b; }
bool operator>=(DataPtr a, DataPtr b)  { return *(a.get()) >= b; }*/

extern DataPtr NULLData;

/*bool operator==(DataPtr a, DataPtr b)  { return *(a.get()) == *(b.get()); }
bool operator!=(DataPtr a, DataPtr b)  { return *(a.get()) != *(b.get()); }
bool operator< (DataPtr a, DataPtr b)  { return *(a.get()) < *(b.get()); }
bool operator<=(DataPtr a, DataPtr b)  { return *(a.get()) <= *(b.get()); }
bool operator> (DataPtr a, DataPtr b)  { return *(a.get()) > *(b.get()); }
bool operator>=(DataPtr a, DataPtr b)  { return *(a.get()) >= *(b.get()); }*/

/*****************
 ***** Tuple *****
 *****************/

// A named Tuple, which maps string names to DataPtr values
class Tuple;
typedef SharedPtr<Tuple> TuplePtr;
class TupleSchema;
typedef SharedPtr<const TupleSchema> ConstTupleSchemaPtr;
class Tuple : public Data {
  public:
  std::vector<DataPtr> tFields;
  Tuple(ConstTupleSchemaPtr schema);
  Tuple(const std::vector<DataPtr>& tFields, ConstTupleSchemaPtr schema);
  	
  const std::vector<DataPtr>& getFields() const;
  
  // Appends the given Data object after the last field in this tuple
  void add(DataPtr obj, ConstTupleSchemaPtr schema);
  
  // Maps the given field to the given data object. Returns whether a data object was previously
  // mapped at this index.
  bool add(unsigned int idx, DataPtr obj, ConstTupleSchemaPtr schema);
  
  // Returns a shared pointer to the data at the given field within the Tuple, or
  // NULLDataPtr if this field does not exist.
  DataPtr get(unsigned int idx, ConstTupleSchemaPtr schema) const;

  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const DataPtr& that_arg) const;
  virtual bool operator==(const TuplePtr& that) const;

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const DataPtr& that_arg) const;
  bool operator<(const TuplePtr that) const;

  // Call the parent class's getName call and then Append this class' unique name 
  // to the name list.
  void getName(std::list<std::string>& name) const;
  	
  //properties* serializeTuple(properties* props);
  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(Tuple, Data)
  
  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;
}; // class Tuple

/******************
 ***** Record *****
 ******************/

// A named record, which maps string names to DataPtr values
class Record;
typedef SharedPtr<Record> RecordPtr;
class RecordSchema;
typedef SharedPtr<const RecordSchema> ConstRecordSchemaPtr;
class Record : public Data {
  public:
  /*typedef easymap<std::string, DataPtr> fields;
  std::map<std::string, DataPtr> rFields;*/
  // The fields of this record. The labels associated with each field are maintained in the RecordSchema 
  // that this Record is associated with.
  std::vector<DataPtr> rFields;
  Record(ConstRecordSchemaPtr schema);
  Record(const std::map<std::string, DataPtr>& label2field, ConstRecordSchemaPtr schema);
  	
  const std::vector<DataPtr>& getFields() const;
  std::map<std::string, DataPtr> getFieldsMap(ConstRecordSchemaPtr schema) const;
  
  // Maps the given field name to the given data object.
  void add(const std::string& label, DataPtr obj, ConstRecordSchemaPtr schema);
  
  // Returns a shared pointer to the data at the given field within the record, or
  // NULLDataPtr if this field does not exist.
  DataPtr get(const std::string& label, ConstRecordSchemaPtr schema) const;

  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const DataPtr& that_arg) const;
  virtual bool operator==(const RecordPtr& that) const;

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const DataPtr& that_arg) const;
  bool operator<(const RecordPtr that) const;

  // Call the parent class's getName call and then Append this class' unique name 
  // to the name list.
  void getName(std::list<std::string>& name) const;
  	
  //properties* serializeRecord(properties* props);
  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(Record, Data)
  
  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;
}; // class Record

/******************
 ***** KeyVal *****
 ******************/
class KeyValSchema; 
typedef SharedPtr<KeyValSchema> KeyValSchemaPtr;
typedef SharedPtr<const KeyValSchema> ConstKeyValSchemaPtr;

// General interface for implementations for key->value mappings.
// A mapping is an un-ordered set of pairs of Records, such as 
//    - key: index in an array
//      value: value at that index
//    - key: graph edge
//      value: weight of that edge
//    - key: sparse matrix
//      value: energy use of a linear solver run on this matrix
// Each value in the key and value record in each pair may be 
// an arbitrary Data item, which allows for the creation of complex 
// data structures
// key->value mappings are opaque but must allow the following 
//    generic access mechanisms:
//    - map: apply a functor over the all the key->value pairs
//    - alignMap: join the pairs of two instances of the same KeyValMap 
//      sub-type so that all the functor is called each pair in the two
//      maps that share the same key
//    - aggregate: add more key->value pairs from one map
//      to another (must have identical types)
class KeyValMap;
typedef SharedPtr<KeyValMap> KeyValMapPtr;
class KeyValMap : public Data {
  public:
  /* // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const DataPtr& that_arg) const {
    KeyValMapPtr that = dynamicPtrCast<KeyValMap>(that_arg);
    if(!that) { cerr << "KeyValMap::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
    return *this == that;
  }
  virtual bool operator==(const KeyValMapPtr& that) const
  { return fields == that->fields; }

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const DataPtr& that_arg) const {
    KeyValMapPtr that = dynamicPtrCast<KeyValMap>(that_arg);
    if(!that) { cerr << "KeyValMap::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
    return *this < that;
  }
  bool operator<(const KeyValMapPtr that) const
  { return fields < that->fields; }*/  

  // Call the parent class's getName call and then Append this class' unique name 
  // to the name list.
  void getName(std::list<std::string>& name) const {
    Data::getName(name);
    name.push_back("KeyValMap");
  }

  // Functor that can be applied to each key->value pair. 
  class mapFunc {
    public:
    // Called on every key->value pair in the given map
    virtual void map(const DataPtr& key, const DataPtr& value)=0;

    // Called when the iteration has completed
    virtual void iterComplete()=0;
  }; // class mapFunc

  // Iterate over the key->value pairs internal to a compound object
  virtual void map(mapFunc& mapper) const=0;

  // Functor that can be applied to all the key->value mappings 
  // in a set of KeyValMap in the intersection of these maps' keys
  class commonMapFunc {
    public:
    // Called on every key->value pair in a set of maps, 
    // where the key is identical
    // key - the key that is shared by the pairs in the different maps
    // values - vector of all the values from the different maps
    //    that have this key, one from each map
    virtual void map(const DataPtr& key, 
                     const std::vector<DataPtr>& values)=0;

    // Called when the iteration has completed
    virtual void iterComplete()=0;
  }; // class commonMapFunc 

  // Join the key->value mappings in all the KeyValMaps in kvMaps. The join is the cross-product
  // of the mappings from each KeyValMap kvMaps have the same key. This object may be contained
  // in kvMaps vector but does not have to be. All the KeyValMaps in kvMaps must have the same
  // type as this.
  // mapSchema: the schema of each of the maps (must be the same)
  virtual void alignMap(commonMapFunc & mapper, 
                        const std::vector<KeyValMapPtr>& kvMaps,
                        KeyValSchemaPtr mapSchema) const=0;

  // Updates this object to include all the data from the given object
  virtual void aggregate(KeyValMapPtr that)=0;

/*  properties* serializeKeyValMap (properties* props) {
    return props;
  }*/

  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(KeyValMap, Data)
}; // class KeyValMap 

/**************************
 ***** ExplicitKeyVal *****
 **************************/

// Implementation of KeyValMap in terms of an explicit mapping of each
// key record to all the value records it is associated with
class ExplicitKeyValMap;
typedef SharedPtr<ExplicitKeyValMap> ExplicitKeyValMapPtr;
typedef SharedPtr<const ExplicitKeyValMap> ConstExplicitKeyValMapPtr;
class ExplicitKeyValMap : public KeyValMap {
	std::map<DataPtr, std::list<DataPtr> > data;
  public:
  ExplicitKeyValMap();
  ExplicitKeyValMap(const DataPtr& key, const DataPtr& value);
  
  const std::map<DataPtr, std::list<DataPtr> >& getData() const { return data; }
  std::map<DataPtr, std::list<DataPtr> >& getDataMod() { return data; }
  	
  // Maps the given key to the given list of values object, returning whether this mapping overrides a prior 
  // one (true) or is a fresh mapping (false).
  bool add(DataPtr key, const std::list<DataPtr> value);

  // Adds the given key->value pair to the list
  void add(const DataPtr& key, const DataPtr& value);

  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const DataPtr& that_arg) const;

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const DataPtr& that_arg) const;

  // Call the parent class's getName call and then Append this class' unique name 
  // to the name list.
  void getName(std::list<std::string>& name) const;

  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(ExplicitKeyValMap, Data)
  
  // Iterate over the key->value pairs internal to a compound object
  void map(mapFunc& mapper) const;
  
  // Applies the given commonMapFunc on the cross-produce of the value lists associated with the given key
  void mapCrossProduct(DataPtr key,
                       vector<list<DataPtr>::const_iterator>& curLoc,
                       vector<list<DataPtr>::const_iterator>& endLoc, 
                       unsigned int idx, commonMapFunc & mapper) const;

  // Join the key->value mappings in all the KeyValMaps in kvMaps. The join is the cross-product
  // of the mappings from each KeyValMap kvMaps have the same key. This object may be contained
  // in kvMaps vector but does not have to be. All the KeyValMaps in kvMaps must have the same
  // type as this.
  // mapSchema: the schema of each of the maps (must be the same)
  void alignMap(commonMapFunc & mapper, 
                const std::vector<KeyValMapPtr>& kvMaps,
                KeyValSchemaPtr mapSchema) const;

  // Updates this object to include all the data from the given object
  void aggregate(KeyValMapPtr that_arg);

  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;
}; // class ExplicitKeyValMap

/******************
 ***** Scalar *****
 ******************/

// Implementation of KeyValMap in terms of an explicit mapping of each
// key record to all the value records it is associated with
class ScalarSchema;
typedef SharedPtr<const ScalarSchema> ConstScalarSchemaPtr;

template<typename T>
class Scalar : public Data {  
  T val;
  public:
  Scalar();
  Scalar(const T& val);

  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(ScalarData<T>, Data)

  void set(const T& newVal);
  const T& get() const;
  T& getMod();
  void merge(const DataPtr& that_arg);


  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const DataPtr& that_arg) const;
  virtual bool operator==(const SharedPtr<Scalar<T> >& that) const;

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const DataPtr& that_arg) const;
  bool operator<(const SharedPtr<Scalar<T> > that) const;
  
  // Call the parent class's getName call and then Append this class' unique name 
  // to the name list.
  void getName(std::list<std::string>& name) const;

  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  bool operator==(const Data& that_arg) const;

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  bool operator<(const Data& that_arg) const;

  // Write a human-readable string representation of this object to the given
  // output stream
  std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;
}; // class Scalar


/*************************
***** HistogramBin   *****
*************************/

// A named record, which maps string names to DataPtr values
class HistogramBin;
typedef SharedPtr<HistogramBin> HistogramBinPtr;
class HistogramBinSchema;
typedef SharedPtr<const HistogramBinSchema> ConstHistogramBinSchemaPtr;

class HistogramBin : public Data {
public:

    DataPtr count ;
    DataPtr start;
    DataPtr end ;
    HistogramBin(ConstHistogramBinSchemaPtr schema);
    HistogramBin();

    //accessor functions
    DataPtr getCount(){ return count ;}
    DataPtr getStartPos(){ return start ;}
    DataPtr getEndPos(){ return end ;}

    void merge (const DataPtr& that_arg);

    void update (int count);
    // Maps the given field name to the given data object.
    void add(const std::string& label, DataPtr obj, ConstHistogramBinSchemaPtr schema);

    // Returns a shared pointer to the data at the given field within the record, or
    // NULLDataPtr if this field does not exist.
    DataPtr get(const std::string& label, ConstHistogramBinSchemaPtr schema) const;

    // Return whether this object is identical to that object
    // that must have a name that is compatible with this
    bool operator==(const DataPtr& that_arg) const;
    virtual bool operator==(const HistogramBinPtr& that) const;

    // Return whether this object is strictly less than that object
    // that must have a name that is compatible with this
    bool operator<(const DataPtr& that_arg) const;
    bool operator<(const HistogramBinPtr that) const;

    // Call the parent class's getName call and then Append this class' unique name
    // to the name list.
    void getName(std::list<std::string>& name) const;

    //properties* serializeRecord(properties* props);
    // Defines this class and its places in its inheritance hierarchy
    //DATA_DEFS(Record, Data)

    // Write a human-readable string representation of this object to the given
    // output stream
    std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;
}; // class Record

/**********************
***** Histogram   *****
***********************/

class Histogram;
typedef SharedPtr<Histogram> HistogramPtr;
typedef SharedPtr<RecordSchema> RecordSchemaPtr;
class HistogramSchema;
typedef SharedPtr<HistogramSchema> HistogramSchemaPtr;

class Histogram : public Data {
    //bin map for this histogram
    //stores all the bins for a particular histogram
    //key is the starting range of a respective bin
    std::map<DataPtr, std::list<DataPtr> > data;

    DataPtr minValue;
    DataPtr maxValue;


public:
    Histogram();

    //histogram is not initialized if min/max values are not set !!
    bool isInitialized();

    void setMin(DataPtr& min);
    void setMax(DataPtr& min);

    DataPtr& getMin();
    DataPtr& getMax();

    const std::map<DataPtr, std::list<DataPtr> >& getData() const { return data; }
    std::map<DataPtr, std::list<DataPtr> >& getDataMod() { return data; }


    // Adds a Bin to the binmap
    //this opeartion works as follows
    // a.lookup if HistogramBin exist for 'key'
    //  a.1 if not found then insert Bin
    //  a.2 else perform aggregate 'value' bin with the existing bin
    void aggregateBin(const DataPtr& key, const DataPtr& value);

    void join(HistogramPtr& other);

    // Return whether this object is identical to that object
    // that must have a name that is compatible with this
    bool operator==(const DataPtr& that_arg) const;

    // Return whether this object is strictly less than that object
    // that must have a name that is compatible with this
    bool operator<(const DataPtr& that_arg) const;

    // Call the parent class's getName call and then Append this class' unique name
    // to the name list.
    void getName(std::list<std::string>& name) const;

    // Defines this class and its places in its inheritance hierarchy
    //DATA_DEFS(ExplicitKeyValMap, Data)

    // Write a human-readable string representation of this object to the given
    // output stream
    std::ostream& str(std::ostream& out, ConstSchemaPtr schema) const;

};

/* Implementation of an n-dimensional dense array KeyValMap, where the keys are n-dim 
 * numeric Records and the values are arbitrary Records. 
 * Such dense arrays are built incrementally, so the initial data sources may produce many small 
 * boxes for their respective regions of the n-dim space and the final result is the large dense box that
 * covers the entire n-dim space. However, while these boxes are being collected the intermediate 
 * state may be a large number of non-contiguous dense boxes. As such, the nDimDenseArray 
 * maintains a set of dense boxes and attempts to reduce storage costs by looking for sub-sets of 
 * boxes that form contiguous slabs in the n-dim space, which can be coalesced into a single box.
 */
/*class nDimDenseArray : public KeyValMap { 
  // The set of boxes currently being maintained, with their location in the n-dim space as the key 
  // and their contents and dimensions as the value
  std::map<nDimDenseArrayname::dims, 
            std::pair<nDimDenseArrayname::dims, DataPtr> > values;

  // The dimensionality of the overall space spanned by this data source
  nDimDenseArrayname ::dims spaceDims;
  
  public:
  nDimDenseArray (nDimDenseArrayname ::dims spaceDims) : spaceDims(spaceDims) {}

  // Defines this class and its places in its inheritance hierarchy
  //DATA_DEFS(ExplicitKeyValMap, Data)

  // Registers an n-dimensional array with this object
	  void add(nDimDenseArrayname ::dims offset, nDimDenseArrayname ::dims size, 
           DataPtr obs) {
    // Boxes may not overlap
    if(values.find(offset) != values.end()) { cerr << "nDimDenseArray ::add() ERROR: overlapping boxes!"<<endl; assert(0); }
    // Other overlap checks here

    // Look for opportunities to merge

    // Could not merge this box with others, so add it separately
    values[offset] = make_pair(size, obs);
  }
  
  // Return whether this object is identical to that object
  // that must have a name that is compatible with this
  virtual bool operator==(const Data& that_arg) {
    try {
      const nDimDenseArrayname & that = dynamic_cast<const nDimDenseArrayname &>(that_arg);
      if(spaceDims != that.spaceDims) return false;
      if(values.size() != that.values.size()) return false;

      for(std::map<nDimDenseArrayname ::dims, std::pair<nDimDenseArrayname ::dims, DataPtr >  >::const_iterator  itThis=values.begin(), itThat=that.values.begin();
        itThis!=values.end(); ++itThis, ++itThat) {
        if(itThis->first != itThat->first) return false;
        if(itThis->second.first != itThat->second.first) return false;
        // Iterate over the n-dimensional boxes itThis->second.second and itThat->second.second to check for inequalities
        ..
      }
      return true;
    } catch (std::bad_cast& bc)
    { return false; }  }
  }

  // Return whether this object is strictly less than that object
  // that must have a name that is compatible with this
  virtual bool operator<(const Data& that) {
    try {
      const nDimDenseArrayname & that = dynamic_cast<const nDimDenseArrayname &>(that_arg);
      if(spaceDims < that.spaceDims) return true;
      if(spaceDims > that.spaceDims) return false;
      if(values.size() < that.values.size()) return true;
      if(values.size() > that.values.size()) return false;
      
      for(std::map<nDimDenseArrayname ::dims, std::pair<nDimDenseArrayname ::dims, DataPtr >  >::const_iterator  itThis=values.begin(), itThat=that.values.begin();
        itThis!=values.end(); ++itThis, ++itThat) {
        if(itThis->first < itThat->first) return true;
        if(itThis->first < itThat->first) return false;

        if(itThis->second.first < itThat->second.first) return true;
        if(itThis->second.first > itThat->second.first) return false;
        // Iterate over the n-dimensional boxes itThis->second.second and itThat->second.second to check for inequalities
        ..
      }
      return true;
    } catch (std::bad_cast& bc)
    { return false; }  }
  }

  // Iterate over the key->value pairs internal to a compound object
  void map(mapFunc& mapper) const {
    for(typename map<DataPtr, list<DataPtr> >::const_iterator i=data.begin(); i!=data.end(); i++) {
      for(list<DataPtr>::const_iterator j=i->second.begin(); j!=i->second.end(); j++) {
        mapper.map(i->first, *j);
      }
    }
    map.iterComplete();
  }

  // Iterate over the key->value pairs in this object and all objects in that, which 
  // share a common key
  virtual void map(commonMapFunc & mapper, const std::vector<const Data&> that) {
     All keys are sorted so iterate in sorted order, call map on common keys
  }

  // Updates this object to include all the data from the given object
  void aggregate(const KeyValMap& that) {
      Add each pair in that into this
  }
}; // class nDimDenseArray
typedef SharedPtr<nDimDenseArray> nDimDenseArrayPtr;
*/

