#pragma once 

#include <stdlib.h>
#include <stdio.h>
#include <map>
#include <list>
#include <iostream>
#include <string>
#include <string.h>
#include <errno.h>
//#include "sight_common_internal.h"
//#include "sight_layout.h"

#include <boost/enable_shared_from_this.hpp>
#include <boost/make_shared.hpp>

namespace sight {
  
// Records the properties of a given object
class properties;
typedef boost::shared_ptr<properties> propertiesPtr;
class properties
{
  public:
  // Differentiates between the entry tag of an object and its exit tag
  typedef enum {enterTag, exitTag, unknownTag} tagType;
  
  // Lists the mapping the name of a class in an inheritance hierarchy to its map of key-value pairs.
  // Objects are ordered according to inheritance depth with the base class at the end of the 
  // list and most derived class at the start.
  std::list<std::pair<std::string, std::map<std::string, std::string> > > p;
  
  // Records whether this object is active (true) or disabled (false)
  bool active;
  
  // Records whether a tag should be emitted for this object
  bool emitTag;
  
  // Records the properties of any objects nested inside this one (optional).
  // Note that the contents are stored once, not once per for every element in p 
  // (typically one element for every level in an object's inheritance hierarchy)
  // This will be improved in the future.
  std::list<propertiesPtr> contents;
  
  properties(): active(true), emitTag(true) {}
  // Creates properties where the object name objName is mapped to no properties
  properties(std::string objName): active(true), emitTag(true)  {
    std::map<std::string, std::string> emptyMap;
    add(objName, emptyMap);
  }
  properties(const std::list<std::pair<std::string, std::map<std::string, std::string> > >& p, const bool& active, const bool& emitTag): p(p), active(active), emitTag(emitTag) {}
  properties(const properties& that) : p(that.p), active(that.active), emitTag(that.emitTag) {}
    
  void add(std::string className, const std::map<std::string, std::string>& props);
  
  // Add a properties object to the contents of this object
  void addSubProp(propertiesPtr sub);
  
  // Records the properties of any objects nested inside this one (optional)
  const std::list<propertiesPtr>& getContents() const { return contents; }
  
  bool operator==(const properties& that) const
  { 
    /*std::cout << "p==that.p = "<<(p==that.p)<<std::endl;
      std::cout << "p.size()="<<p.size()<<", that.p.size()="<<that.p.size()<<std::endl;
      std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator itThis = p.begin();
      std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator itThat = that.p.begin();
      for(; itThis!=p.end(); itThis++, itThat++) {
        std::cout << "    itThis->first = "<<itThis->first<<" itThat->first="<<itThat->first<<std::endl;
        std::cout << "    itThis->first==itThat->first = "<<(itThis->first==itThat->first)<<", itThis->second==itThat->second = "<<(itThis->second==itThat->second)<<std::endl;
      }
    std::cout << "active==that.active = "<<(active==that.active)<<std::endl;
    std::cout << "emitTag==that.emitTag = "<<(emitTag==that.emitTag)<<std::endl;*/
    return p==that.p && active==that.active && emitTag==that.emitTag; }

  bool operator!=(const properties& that) const
  { return !(*this == that); }
  
  bool operator<(const properties& that) const
  { return (p< that.p) ||
           (p==that.p && active< that.active) ||
           (p==that.p && active==that.active && emitTag< that.emitTag); }
  
  // Returns the text that should be emitted to the structured output file that denotes the the entry into a tag. 
  // The tag is set to the given property key/value pairs
  std::string enterStr() const;
  
  // Returns the text that should be emitted to the the structured output file to that denotes exit from a given tag
  //std::string dbgStream::exitStr(std::string name) {
  std::string exitStr() const;
  
  // Returns the text that should be emitted to the the structured output file to that denotes a full tag an an the structured output file
  //std::string dbgStream::tagStr(std::string name, const std::map<std::string, std::string>& properties, bool inheritedFrom) {
  std::string tagStr() const;
  
  //typedef std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator iterator;
  
  // Wrapper for iterators to property lists that includes its own end iterator to make it possible to 
  // tell whether the iterator has reached the end of the list without having a reference to the list itself.
  class iterator {
    friend class properties;
    std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator cur;
    std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator end;
    const properties& parent;
    
    public:  
    iterator(const std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator& cur,
             const std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator& end,
             const properties& parent) :
        cur(cur), end(end), parent(parent)
    {}
    
    iterator(const std::list<std::pair<std::string, std::map<std::string, std::string> > >& l, const properties& parent) :
      cur(l.begin()), end(l.end()), parent(parent)
    {}
    
    iterator(const properties& props) :
      cur(props.p.begin()), end(props.p.end()), parent(props)
    {}
    
    // Returns the value mapped to the given key
    std::string get(std::string key) const;
    
    // Returns the integer interpretation of the value mapped to the given key
    long getInt(std::string key) const;

    // Returns the floating-point interpretation of the value mapped to the given key
    double getFloat(std::string key) const;
    
    // Returns the list of properties of any objects nested inside this one.
    // Currently there is one such list for all the iterators within a given properties 
    // object and this should be corrected in the future.
    const std::list<propertiesPtr>& getContents() const { return parent.contents; }
    
    iterator& operator++() {
      cur++;
      return *this;
    }
    
    iterator& operator++(int) {
      cur++;
      return *this;
    }
    
    // Returns the iterator that follows this one without modifying this one
    iterator next() const {
      std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator next = cur;
      ++next;
      return iterator(next, end, parent);
    }
    
    // Returns the iterator that precedes this one without modifying this one
    iterator prev() const {
      std::list<std::pair<std::string, std::map<std::string, std::string> > >::const_iterator prev = cur;
      --prev;
      return iterator(prev, end, parent);
    }
    
    const std::pair<std::string, std::map<std::string, std::string> >& operator*() {
      assert(!isEnd());
      return *cur;
    }
    
    // Returns whether this iterator has reached the end of its list
    bool isEnd() const
    { return cur == end; }
    
    // Given an iterator to a particular key->value mapping, returns the number of keys in the map
    int getNumKeys() const
    { return cur->second.size(); }
      
    // Given an iterator to a particular key->value mapping, returns a const reference to the key/value mapping
    const std::map<std::string, std::string>& getMap() const
    { return cur->second; }
    
    public:
    
    // Returns whether the given key is mapped to a value in the key/value map at this iterator
    bool exists(std::string key) const
    { return cur->second.find(key) != cur->second.end(); }
    
    // Returns the name of the object type referred to by the given iterator
    std::string name() const
    { return cur->first; }
    
    // Returns the string representation of the given properties iterator  
    std::string str() const;
  }; //class iterator
  
  // Returns the start of the list to iterate from the most derived class of an object to the most base
  iterator begin() const;
  
  // The corresponding end iterator
  iterator end() const;
  
  // Returns the iterator to the given objectName
  iterator find(std::string name) const;
  
  // Given a properties iterator returns an iterator that refers to the next position in the list
  static iterator next(iterator i);
  
  // Given an iterator to a particular key->value mapping, returns the value mapped to the given key
  static std::string get(iterator cur, std::string key);
  
  // Given the label of a particular key->value mapping, adds the given mapping to it
  void set(std::string name, std::string key, std::string value);
  
  // Given an iterator to a particular key->value mapping, returns the integer interpretation of the value mapped to the given key
  static long getInt(iterator cur, std::string key);
  
  // Returns the integer interpretation of the given string
  static long asInt(std::string val);
  
  // Given an iterator to a particular key->value mapping, returns the floating-point interpretation of the value mapped to the given key
  static double getFloat(iterator cur, std::string key);
  
  // Returns the floating-point interpretation of the given string
  static long asFloat(std::string val);
  
  /* // Given an iterator to a particular key->value mapping, returns whether the given key is mapped to some value
  static bool exists(iterator cur, std::string key);
  
  // Given an iterator to a particular key->value mapping, returns the number of keys in the map
  static int getNumKeys(iterator cur);
    
  // Given an iterator to a particular key->value mapping, returns a const reference to the key/value mapping
  static const std::map<std::string, std::string>& getMap(iterator cur);
    
  // Returns the name of the object type referred to by the given iterator
  static std::string name(iterator cur);*/
  
  // Returns the name of the most-derived class 
  std::string name() const;
  
  // Returns the number of tags recorded in this object
  int size() const;
  
  // Erases the contents of this object
  void clear();
  
  // Returns the string representation of the given properties iterator  
  static std::string str(iterator props);
  
  std::string str(std::string indent="") const;

  // Converts a tagType to its string representation
  static std::string tagType2Str(tagType tt)
  { return (tt==enterTag?"enterTag":(tt==exitTag?"exitTag":(tt==unknownTag?"unknownTag":"???"))); }
}; // class properties

extern propertiesPtr NULLProperties;

class structureParser {
  public:
 
  // Reads more data from the data source, returning the type of the next tag read and the properties of 
  // the object it denotes.
  virtual std::pair<properties::tagType, propertiesPtr> next()=0;
    
  // Reads the next tag from the data source, including all the tags that it contains
  // (only info for the entry tags is maintained since the exit tags are assumed to maintain
  //  no information other than their matching to enter tags)
  virtual propertiesPtr nextFull()=0;
};

template<typename streamT>
class baseStructureParser : public structureParser {
  protected:
  // Holds the data just read from the data source
  char* buf;
  
  // Allocated size of buf
  size_t bufSize;
  
  // Number of bytes currently stored in buf
  size_t dataInBuf;
  
  // The current index of the read pointer within buf[]
  int bufIdx;
  
  // Reference to the data source
  streamT* stream;
  
  // Functions implemented by children of this class that specialize it to take input from various sources.
  
  // readData() reads as much data as is available from the data source into buf[], upto bufSize bytes 
  // and returns the amount of data actually read.
  virtual size_t readData()=0;
  
  // Returns true if we've reached the end of the input stream
  virtual bool streamEnd()=0;
  
  // Returns true if we've encountered an error in input stream
  virtual bool streamError()=0;

  public:  
  baseStructureParser(int bufSize=10000);
  baseStructureParser(streamT* stream, int bufSize=10000);
  void init(streamT* stream);
  
  protected:
  // The location within nextLoc() at which the last call to newLoc() stopped and the
  // next newLoc() call will resume.
  typedef enum {start,
                textRead,
                enterTagRead,
                exitTagRead,
                done} codeLoc;
  codeLoc loc;
  
  // The properties of the object tag that is being currently read
  //properties tagProperties;
    
  public:
  // Reads more data from the data source, returning the type of the next tag read and the properties of 
  // the object it denotes.
  std::pair<properties::tagType, propertiesPtr> next();
  
  // Reads the next tag from the data source, including all the tags that it contains
  // (only info for the entry tags is maintained since the exit tags are assumed to maintain
  //  no information other than their matching to enter tags)
  propertiesPtr nextFull();
  
  protected:
  // Read a property name/value pair from the given file, setting name and val to them.
  // Reading starts at buf[bufIdx] and continues as far as needed, reading more file 
  // contents into buf if the end of buf is reached. bufSize is the number of bytes in 
  // the buffer. When the function returns bufIdx is updated to hold the current 
  // position inside buf, immediately after the terminator char was encountered and 
  // bufSize is updated to hold the total number of characters currently in buf. 
  // termChar is set to be the character that immediately follows the property name/value.
  // pair. This information enables the caller to determine whether the tag is finished.
  // or whether more properties exist in the tag. If readProperty() reads the full 
  // property info before it reaches the end of the file, it returns true and false otherwise.
  bool readProperty(std::string& name, std::string& val, char& termChar);
  
  // Reads all the characters from FILE* f until the next terminating character.
  // If inTerm is true, a terminating char is anything in termChars. If inTerm is 
  // false, a terminating char is anything not in termChars. readUntil() then sets 
  // result to the read text and sets termHit to the terminator char that was hit. 
  // Reading starts at buf[bufIdx] and continues as far as needed, reading more file 
  // contents into buf if the end of buf is reached. bufSize is the number of bytes in 
  // the buffer. When the function returns bufIdx is updated to hold the current 
  // position inside buf, immediately after the terminator char was encountered and 
  // bufSize is updated to hold the total number of characters currently in buf. If 
  // readUntil() finds a terminator char before it reaches the end of the file, it 
  // returns true and false otherwise.
  bool readUntil(bool inTerm, const char* termChars, int numTermChars, 
                 char& termHit, std::string& result);
  
  // Advances the current reading position by 1 char, reading file contents into buf 
  // if the end of buf is reached. bufSize is the number of bytes in 
  // the buffer. When the function returns bufIdx is updated to hold the current 
  // position inside buf, immediately after the terminator char was encountered and 
  // bufSize is updated to hold the total number of characters currently in buf. 
  // Returns the read char. If there is a next character in the file, returns true.
  // Otherwise, returns false.
  bool nextChar();
  
  // Returns true if character c is in array termChars of size numTermChars and 
  // false otherwise.
  static bool isMember(char c, const char* termChars, int numTermChars);
};


class FILEStructureParser : public baseStructureParser<FILE> {
  // Records whether this object opened the file on its own (in which case it needs to close it)
  // or was given a ready FILE* stream
  bool openedFile;
  
  public:
  FILEStructureParser(std::string fName, int bufSize=10000);
  FILEStructureParser(FILE* f, int bufSize=10000);
  ~FILEStructureParser();
  
  protected:
  // Functions implemented by children of this class that specialize it to take input from various sources.
  
  // readData() reads as much data as is available from the data source into buf[], upto bufSize bytes 
  // and returns the amount of data actually read.
  size_t readData();
  
  // Returns true if we've reached the end of the input stream
  bool streamEnd();
  
  // Returns true if we've encountered an error in input stream
  bool streamError();
};
} // namespace sight