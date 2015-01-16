#define DATA_C
#include "schema.h"
#include "data.h"
#include <vector>
using namespace std;

DataPtr NULLData;

/******************
 ***** Tuple *****
 ******************/

Tuple::Tuple(ConstTupleSchemaPtr schema) {
  tFields.reserve(schema->tFields.size());
}

Tuple::Tuple(const std::vector<DataPtr>& tFields, const ConstTupleSchemaPtr schema) : tFields(tFields) {
}

const std::vector<DataPtr>& Tuple::getFields() const 
{ return tFields; }

// Appends the given Data object after the last field in this tuple.
void Tuple::add(DataPtr obj, const ConstTupleSchemaPtr schema) {
  tFields.push_back(obj);
}

// Maps the given field to the given data object. Returns whether a data object was previously
// mapped at this index.
bool Tuple::add(unsigned int idx, DataPtr obj, const ConstTupleSchemaPtr schema) {
  tFields.reserve(idx+1);
  bool previouslyMapped = (tFields[idx].get()!=NULL);
  tFields[idx] = obj;
  return previouslyMapped;
}
  	
// Returns a shared pointer to the data at the given field within the Tuple, or
// NULLDataPtr if this field does not exist.
DataPtr Tuple::get(unsigned int idx, const ConstTupleSchemaPtr schema) const {
  if(idx>=tFields.size()) return NULLData;
  else return tFields[idx];
}

// Return whether this object is identical to that object
// that must have a name that is compatible with this
bool Tuple::operator==(const DataPtr& that_arg) const {
  TuplePtr that = dynamicPtrCast<Tuple>(that_arg);
  if(!that) { cerr << "Tuple::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this == that;
}
bool Tuple::operator==(const TuplePtr& that) const
{ return tFields == that->tFields; }

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool Tuple::operator<(const DataPtr& that_arg) const {
  TuplePtr that = dynamicPtrCast<Tuple>(that_arg);
  if(!that) { cerr << "Tuple::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this < that;
}
bool Tuple::operator<(const TuplePtr that) const
{ return tFields < that->tFields; }

// Call the parent class's getName call and then Append this class' unique name 
// to the name list.
void Tuple::getName(std::list<std::string>& name) const {
  Data::getName(name);
  name.push_back("Tuple");
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& Tuple::str(std::ostream& out, ConstSchemaPtr schema_arg) const {
  ConstTupleSchemaPtr schema = dynamicPtrCast<const TupleSchema>(schema_arg);
  assert(tFields.size() == schema->tFields.size());
  out << "[Tuple: tFields="<<endl;
  typename vector<DataPtr>::const_iterator d=tFields.begin();
  typename vector<SchemaPtr>::const_iterator s=schema->tFields.begin();
  unsigned int i=0;
  for(; d!=tFields.end(); ++d, ++s, ++i) {
    out << "    "<<i<<": "; (*d)->str(out, *s); cout<<endl;
  }
  out << "]";
  return out;
}

/******************
 ***** Record *****
 ******************/

Record::Record(ConstRecordSchemaPtr schema) {
  assert(schema->schemaFinalized);
  rFields.resize(schema->rFields.size());
}

Record::Record(const std::map<std::string, DataPtr>& label2field, const ConstRecordSchemaPtr schema) {
  assert(schema->schemaFinalized);
  assert(label2field.size() == schema->rFields.size());
  rFields.reserve(schema->rFields.size());
  for(map<string, DataPtr>::const_iterator f=label2field.begin(); f!=label2field.end(); ++f) {
    rFields[schema->getIdx(f->first)] = f->second;
  }
}

const std::vector<DataPtr>& Record::getFields() const 
{ return rFields; }

std::map<std::string, DataPtr> Record::getFieldsMap(const ConstRecordSchemaPtr schema) const { 
  assert(schema->schemaFinalized);
  map<std::string, DataPtr> m;
  assert(rFields.size() == schema->rFields.size());
  map<string, SchemaPtr>::const_iterator s=schema->rFields.begin();
  for(vector<DataPtr>::const_iterator d=rFields.begin(); d!=rFields.end(); ++d, ++s) {
    m[s->first] = *d;
  }
  return m;
}

// Maps the given field name to the given data object
void Record::add(const std::string& label, DataPtr obj, const ConstRecordSchemaPtr schema) {
  assert(schema->schemaFinalized);
/*  cout << "Record::add() label="<<label<<endl;
  cout << "idx="<<schema->getIdx(label)<<endl;
  cout << "obj="; obj->str(cout); cout<<endl;*/
  rFields[schema->getIdx(label)] = obj;
//  cout << "#rFields="<<rFields.size()<<endl;
}
  	
// Returns a shared pointer to the data at the given field within the record, or
// NULLDataPtr if this field does not exist.
DataPtr Record::get(const std::string& label, const ConstRecordSchemaPtr schema) const {
  assert(schema->schemaFinalized);
  return rFields[schema->getIdx(label)];
}

// Return whether this object is identical to that object
// that must have a name that is compatible with this
bool Record::operator==(const DataPtr& that_arg) const {
  RecordPtr that = dynamicPtrCast<Record>(that_arg);
  if(!that) { cerr << "Record::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this == that;
}
bool Record::operator==(const RecordPtr& that) const
{ return rFields == that->rFields; }

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool Record::operator<(const DataPtr& that_arg) const {
  RecordPtr that = dynamicPtrCast<Record>(that_arg);
  if(!that) { cerr << "Record::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this < that;
}
bool Record::operator<(const RecordPtr that) const
{ return rFields < that->rFields; }

// Call the parent class's getName call and then Append this class' unique name 
// to the name list.
void Record::getName(std::list<std::string>& name) const {
  Data::getName(name);
  name.push_back("Record");
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& Record::str(std::ostream& out, ConstSchemaPtr schema_arg) const {
  ConstRecordSchemaPtr schema = dynamicPtrCast<const RecordSchema>(schema_arg);

  out << "[Record: rFields="<<endl;
  typename vector<DataPtr>::const_iterator d=rFields.begin();
  typename map<string, SchemaPtr>::const_iterator s=schema->rFields.begin();
  for(; d!=rFields.end(); ++d, ++s) {
    cout << "    "<<s->first<<": "; (*d)->str(cout, s->second); cout<<endl;
  }
  out << "]";
  return out;
}

/******************************
 ***** ExplicitKeyValMap *****
 ******************************/

ExplicitKeyValMap::ExplicitKeyValMap() {}
ExplicitKeyValMap::ExplicitKeyValMap(const DataPtr& key, const DataPtr& value) {
  add(key, value);
}

// Maps the given key to the given list of values object, returning whether this mapping overrides a prior 
// one (true) or is a fresh mapping (false).
bool ExplicitKeyValMap::add(DataPtr key, const std::list<DataPtr> value) 
{ 
  typename map<DataPtr, list<DataPtr> >::iterator i=data.find(key);
  data.insert(make_pair(key, value));
  return i!=data.end();
}

// Adds the given key->value pair to the list
void ExplicitKeyValMap::add(const DataPtr& key, const DataPtr& value) {
  // If no entry exists in data for the given key, add one
  if(data.find(key) == data.end()) data[key] = list<DataPtr>();    
  data[key].push_back(value);
}

// Return whether this object is identical to that object
// that must have a name that is compatible with this
bool ExplicitKeyValMap::operator==(const DataPtr& that_arg) const {
  ExplicitKeyValMapPtr that = dynamicPtrCast<ExplicitKeyValMap>(that_arg);
  if(!that) { cerr << "ExplicitKeyValMap::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }

  if(data.size() != that->data.size()) return false;
    
  typename map<DataPtr, list<DataPtr> >::const_iterator thisKeyIt = data.begin(),
                                                        thatKeyIt = that->data.begin();
  for(; thisKeyIt!=data.end(); ++thisKeyIt, ++thatKeyIt) {
    if(thisKeyIt->second.size() != thatKeyIt->second.size()) return false;
    
    list<DataPtr>::const_iterator thisValIt = thisKeyIt->second.begin(),
                                  thatValIt = thatKeyIt->second.begin();
    for(; thisValIt!=thisKeyIt->second.end(); ++thisValIt, ++thatValIt) {
      if(*thisValIt != *thatValIt) return false;  
    }
  }

  return true;
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
bool ExplicitKeyValMap::operator<(const DataPtr& that_arg) const {
  ExplicitKeyValMapPtr that = dynamicPtrCast<ExplicitKeyValMap>(that_arg);
  if(!that) { cerr << "ExplicitKeyValMap::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }

  if(data.size() < that->data.size()) return true;
  if(data.size() > that->data.size()) return false;
    
  typename map<DataPtr, list<DataPtr> >::const_iterator thisKeyIt = data.begin(),
                                                        thatKeyIt = that->data.begin();
  for(; thisKeyIt!=data.end(); ++thisKeyIt, ++thatKeyIt) {
    if(thisKeyIt->second.size() < thatKeyIt->second.size()) return true;
    if(thisKeyIt->second.size() > thatKeyIt->second.size()) return false;
    
    list<DataPtr>::const_iterator thisValIt = thisKeyIt->second.begin(),
                                  thatValIt = thatKeyIt->second.begin();
    for(; thisValIt!=thisKeyIt->second.end(); ++thisValIt, ++thatValIt) {
      if(*thisValIt < *thatValIt) return true;
      if(*thisValIt > *thatValIt) return false;
    }
  }

  // The objects are equal
  return false;
}

// Call the parent class's getName call and then Append this class' unique name 
// to the name list.
void ExplicitKeyValMap::getName(std::list<std::string>& name) const {
  Data::getName(name);
  name.push_back("ExplicitKeyValMap");
}

// Defines this class and its places in its inheritance hierarchy
//DATA_DEFS(ExplicitKeyValMap, Data)

// Iterate over the key->value pairs internal to a compound object
void ExplicitKeyValMap::map(mapFunc& mapper) const {
  for(typename map<DataPtr, list<DataPtr> >::const_iterator i=data.begin(); i!=data.end(); i++) {
    for(list<DataPtr>::const_iterator j=i->second.begin(); j!=i->second.end(); j++) {
      mapper.map(i->first, *j);
    }
  }
  mapper.iterComplete();
}

// Applies the given commonMapFunc on the cross-produce of the value lists associated with the given key
void ExplicitKeyValMap::mapCrossProduct(DataPtr key,
                                        vector<list<DataPtr>::const_iterator>& curLoc,
                                        vector<list<DataPtr>::const_iterator>& endLoc, 
                                        unsigned int idx, commonMapFunc & mapper) const
{
  // If we've passed the last list of DataPtrs
  if(idx==curLoc.size()) {
    // Pull together all the DataPtrs at the current list locations into a vector
    vector<DataPtr> curVals;
    for(vector<list<DataPtr>::const_iterator>::iterator l=curLoc.begin(); l!=curLoc.end(); ++l) {
      curVals.push_back(**l);
    }
    mapper.map(key, curVals);

  // If we're at one of the intermediate lists of DataPtrs
  } else {
    // Iterate through all the DataPtrs in the current list
    for(; curLoc[idx]!=endLoc[idx]; ++curLoc[idx]) {
      mapCrossProduct(key, curLoc, endLoc, idx+1, mapper);
    }
  }
}

// Join the key->value mappings in all the KeyValMaps in kvMaps. The join is the cross-product
// of the mappings from each KeyValMap kvMaps have the same key. This object may be contained
// in kvMaps vector but does not have to be. All the KeyValMaps in kvMaps must have the same
// type as this.
// mapSchema: the schema of each of the maps (must be the same)
void ExplicitKeyValMap::alignMap(commonMapFunc & mapper, 
                                 const std::vector<KeyValMapPtr>& kvMaps,
                                 KeyValSchemaPtr mapSchema) const {
  /*cout << "ExplicitKeyValMap::alignMap()"<<endl;
  for(vector<KeyValMapPtr>::const_iterator i=kvMaps.begin(); i!=kvMaps.end(); ++i)
  { cout << "    "; (*i)->str(cout, mapSchema); cout << endl; }*/

  // Iterate over all the keys in the first KeyValMap in kvMaps
  ExplicitKeyValMapPtr first = dynamicPtrCast<ExplicitKeyValMap>(*kvMaps.begin());
  /*cout << "    first="; first->str(cout, mapSchema); cout << endl;
  cout << "    ----"<<endl;*/
  for(typename map<DataPtr, list<DataPtr> >::const_iterator key=first->data.begin(); key!=first->data.end(); ++key) {
    //cout << "    key="; key->first->str(cout, mapSchema->key); cout << endl;
    // Iterate over all the ither maps in kvMaps to see if they also contain the current key.
    // During the iteration put together vectors of the begin and end iterators to the corresponding value lists.
    vector<list<DataPtr>::const_iterator> beginLoc, endLoc;
      
    // Initialize beginLoc and endLoc with the begin() and end() of the first map's list of values for the current key
    beginLoc.push_back(key->second.begin());
    endLoc.push_back(key->second.end());
      
    bool keyIsCommon = true;
    
    vector<KeyValMapPtr>::const_iterator i=kvMaps.begin(); ++i;
    for(; i!=kvMaps.end(); ++i) {
      ExplicitKeyValMapPtr cur = dynamicPtrCast<ExplicitKeyValMap>(*i);
      //cout << "        cur="; cur->str(cout, mapSchema); cout << endl;
      
      if(!cur) { cerr << "ExplicitKeyValMap::alignMap() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
        
      // If the current key in data doesn't exist in the current ExplicitKeyValMap in kvMaps, break out
      typename map<DataPtr, std::list<DataPtr> >::const_iterator keyIt = cur->data.find(key->first);
      if(keyIt == cur->data.end()) {
        keyIsCommon = false;
        break;
      }
      
      beginLoc.push_back(keyIt->second.begin());
      endLoc.push_back(keyIt->second.end());
    }
    
    //cout << "keyIsCommon="<<keyIsCommon<<endl;;
    // If the key is common to this and all the ExplicitKeyValMaps in kvMaps, apply the 
    // mapper to the cross-product of their values
    // If the current key in this also exists in kvMaps
    if(keyIsCommon)
      mapCrossProduct(key->first, beginLoc, endLoc, /*idx*/ 0, mapper);
  }
  
  // Notify mapper that the iteration has completed
  mapper.iterComplete();
}

// Updates this object to include all the data from the given object
void ExplicitKeyValMap::aggregate(KeyValMapPtr that_arg) {
  ExplicitKeyValMapPtr that = dynamicPtrCast<ExplicitKeyValMap>(that_arg);
  if(!that) { cerr << "ExplicitKeyValMap::aggregate() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }

  // Iterate over each key in that
  for(typename map<DataPtr, list<DataPtr> >::const_iterator iThat=that->data.begin(); iThat!=that->data.end(); iThat++) {
    typename map<DataPtr, list<DataPtr> >::iterator iThis = data.find(iThat->first);
    // If this key does not exist in this, add its values 
    // directly to data
    if(iThis == data.end()) {
      data[iThat->first] = iThat->second;
    // If this key appears in both this and that, append the 
    // values in that to the key's list in this
    } else {
      for(list<DataPtr>::const_iterator obs=iThat->second.begin(); obs!=iThat->second.end(); obs++)
        iThis->second.push_back(*obs);
    }
  }
}

// Write a human-readable string representation of this object to the given
// output stream
std::ostream& ExplicitKeyValMap::str(std::ostream& out, ConstSchemaPtr schema_arg) const {
  ConstExplicitKeyValSchemaPtr schema = dynamicPtrCast<const ExplicitKeyValSchema>(schema_arg);

  out << "[ExplicitKeyValMap: "<<endl;
  for(typename map<DataPtr, list<DataPtr> >::const_iterator key=data.begin(); key!=data.end(); key++) {
    out << "    "; key->first->str(out, schema->key); out<<": "<<endl;
    for(list<DataPtr>::const_iterator value=key->second.begin(); value!=key->second.end(); value++) {
      out << "        "; (*value)->str(out, schema->value); out << endl;
    }
  }
  out << "]";
  return out;
}

/******************
 ***** Scalar *****
 ******************/

template<typename T>
Scalar<T>::Scalar() {}

template<typename T>
Scalar<T>::Scalar(const T& val) : val(val) { }

// Defines this class and its places in its inheritance hierarchy
//DATA_DEFS(ScalarData<T>, Data)

template<typename T>
void Scalar<T>::set(const T& newVal) { val = newVal; }

template<typename T>
const T& Scalar<T>::get() const { return val; }

template<typename T>
T& Scalar<T>::getMod() { return val; }

// Return whether this object is identical to that object
// that must have a name that is compatible with this
template<typename T>
bool Scalar<T>::operator==(const DataPtr& that_arg) const {
  SharedPtr<Scalar<T> > that = dynamicPtrCast<Scalar<T> >(that_arg);
  if(!that) { cerr << "Record::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this == that;
}

template<typename T>
bool Scalar<T>::operator==(const SharedPtr<Scalar<T> >& that) const
{ return val == that->val; }

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
template<typename T>
bool Scalar<T>::operator<(const DataPtr& that_arg) const {
  SharedPtr<Scalar<T> > that = dynamicPtrCast<Scalar<T> >(that_arg);
  if(!that) { cerr << "Record::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
  return *this < that;
}

template<typename T>
bool Scalar<T>::operator<(const SharedPtr<Scalar<T> > that) const
{ return val < that->val; }

// Call the parent class's getName call and then Append this class' unique name 
// to the name list.
template<typename T>
void Scalar<T>::getName(std::list<std::string>& name) const {
  Data::getName(name);
  name.push_back("Scalar");
}
 
// Return whether this object is identical to that object
// that must have a name that is compatible with this
template<typename T>
bool Scalar<T>::operator==(const Data& that_arg) const {
  try {
    const Scalar& that = dynamic_cast<const Scalar&>(that_arg);
    return val==that.val;
  } catch (std::bad_cast& bc)
  { cerr << "Scalar<T>::operator==() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
}

// Return whether this object is strictly less than that object
// that must have a name that is compatible with this
template<typename T>
bool Scalar<T>::operator<(const Data& that_arg) const {
  try {
    const Scalar& that = dynamic_cast<const Scalar&>(that_arg);
    return val<that.val;
  } catch (std::bad_cast& bc)
  { cerr << "Scalar<T>::operator<() ERROR: applying method to incompatible Data objects!"<<endl; assert(0); }
}

// Write a human-readable string representation of this object to the given
// output stream
template<typename T>
std::ostream& Scalar<T>::str(std::ostream& out, ConstSchemaPtr schema_arg) const {
  ConstScalarSchemaPtr schema = dynamicPtrCast<const ScalarSchema>(schema_arg);
  out << "[Scalar: val={"<<val<<"}, type="<<schema->type2Str()<<"]";
  return out;
}

template class Scalar<char>;
template class Scalar<std::string>;
template class Scalar<int>;
template class Scalar<long>;
template class Scalar<float>;
template class Scalar<double>;

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