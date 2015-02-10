#pragma once

#include <stdlib.h>
#include <list>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <assert.h>
#include "process.h"
#include <boost/thread/tss.hpp>

// Include all the definitions that control compilation
//#include "definitions.h"

namespace sight {

namespace common {

// Returns whether log generation has been enabled or explicitly disabled
bool isEnabled();

} // namespace common

// Class that makes it possible to generate string labels by using the << syntax.
// Examples: txt() << "a=" << (5+2) << "!"
//           txt("a=") << (5+2) << "!"
// Since this class is meant to be used by client code, it is placed inside the easier-to-access sight namespace
struct txt : std::string {
  txt() {}
  txt(const std::string& initTxt) {
    if(common::isEnabled()) {
    	 _stream << initTxt;
    	 assign(_stream.str());
    }
  }
  
  template <typename T>
  txt& operator<<(T const& t) {
    if(common::isEnabled()) {
      _stream << t;
      assign(_stream.str());
    }
    return *this;
  }

  std::string str() const { 
    if(common::isEnabled()) return _stream.str();
    else            return "";
  }
 
  std::ostringstream _stream;
};

// Definitions for printable and properties below are placed in the generic sight namespace 
// because there is no chance of name conflicts

class printable
{
  public:
  virtual ~printable() {}
  //virtual void print(std::ofstream& ofs) const=0;
  virtual std::string str(std::string indent="") const=0;
};
// Call the print method of the given printable object
//std::ofstream& operator<<(std::ofstream& ofs, const printable& p);

namespace common {

class nullBuf: public std::streambuf
{
  std::streambuf* baseBuf;
  public:

  virtual ~nullBuf() {};
  // Construct a streambuf which tees output to both input
  // streambufs.
  nullBuf();
  nullBuf(std::streambuf* baseBuf);
  void init(std::streambuf* baseBuf);

private:
  // This nullBuf has no buffer. So every character "overflows"
  // and can be put directly into the teed buffers.
  virtual int overflow(int c);

  virtual std::streamsize xsputn(const char * s, std::streamsize n);

  // Sync buffer.
  virtual int sync();
}; // class nullBuf

// Stream that uses nullBuf to ignore all text written to it
class nullStream : public std::ostream
{
  nullBuf nullB;

  public:
  nullStream(): std::ostream(&nullB) {}
  //nullStream(std::streambuf* buf): std::ostream(buf) {}
}; // nullStream

// An instance of nullStream that apps can write to with low overhead when they do not wish to emit output
extern nullStream nullS;

// Stream that uses dbgBuf
class dbgStream : public std::ostream
{
  public:
  // The title of the file
  std::string title;
  // The root working directory
  std::string workDir;
  // The directory where all images will be stored
  std::string imgDir;
  // The directory that widgets can use as temporary scratch space
  std::string tmpDir;
    
  // The directories in which different widgets store their output. Created upon request by different widgets.
  std::set<std::string>     widgetDirs;
    
  // Creates an output directory for the given widget and returns its path as a pair:
  // <path relative to the current working directory that can be used to create paths for writing files,
  //  path relative to the output directory that can be used inside generated HTML>
  std::pair<std::string, std::string> createWidgetDir(std::string widgetName);
    
  dbgStream(std::streambuf* buf): std::ostream(buf) {}
}; // dbgStream

// Given a string, returns a version of the string with all the control characters that may appear in the
// string escaped to that the string can be written out to Dbg::dbg with no formatting issues.
// This function can be called on text that has already been escaped with no harm.
std::string escape(std::string s);
std::string unescape(std::string s);

// Wrapper for strings in which some characters have been escaped. This is useful for serializing multi-level 
// collection objects, while using the same separator for each level of the encoding.
// escapedStr's are used as follows:
// Serialization: (serialized representations of multiple objects separated by ':')
//   esapedStr str1(obj1.serialize(), ":", escapedStr::unescaped);
//   esapedStr str2(obj2.serialize(), ":", escapedStr::unescaped);
//   esapedStr str3(obj3.serialize(), ":", escapedStr::unescaped);
//   str serialized = str1.escape() + ":" + str2.escape() + ":" str3.escape();
//
// Deserialization: (of above objects)
//   Given str serialized;
//   common::escapedStr es(serialized, ":", escapedStr::escaped);
//   vector<string> fields = es.unescapeSplit(":");
//   assert(fields.size()==3);
//   obj1Type obj1(fields[0]);
//   obj2Type obj2(fields[1]);
//   obj3Type obj3(fields[2]);
class escapedStr {
  std::string s;
  std::string control;
  
  public:
  typedef enum {escaped, unescaped} sourceT;

  escapedStr() {}
  
  // source==unescaped: Creates an escaped string given a regular UNESCAPED string and an explicit list of control characters
  // source==escaped:   Creates an ESCAPED string given an escaped string and an explicit list of control characters
  escapedStr(std::string s_, std::string control, sourceT source);

  // Copy constructor
  escapedStr(const escapedStr& that) : s(that.s) {}
  
  // Searches the string for the first occurrence of the sequence specified by its arguments, starting at pos and returns 
  // the location. The search ignores any matches that cross escaped characters in this string.
  size_t find(std::string sub, size_t pos = 0) const;

  // Searches the string for the first occurrence of any of the characters in string chars, starting at pos and returns 
  // the location. The search ignores any matches that cross escaped characters in this string.
  size_t findAny(std::string chars, size_t pos) const;

  // Returns a newly constructed escaped string object with its value initialized to a copy of a substring of this object.
  // The substring is the portion of the object that starts at character position pos and spans len characters (or until 
  // the end of the string, whichever comes first).
  std::string substr(size_t pos = 0, size_t len = std::string::npos) const;
  
  // Returns the fully unescaped version of this string, with the escaped characters replaced with the originals.
  std::string unescape() const;

  // Split the string into sub-strings separated by any character in the separator string and emit a list of the individual
  // substrings, which have been unescaped. The separator characters must be a subset of this escapedStr's control characters.
  std::vector<std::string> unescapeSplit(std::string separator);

  // Returns the escaped read-only version of the string
  const std::string& escape() const { return s; }
  
  // Assignment 
  escapedStr& operator=(const escapedStr& that);

  // Relations
  bool operator==(const escapedStr& that) { return s == that.s; }
  bool operator< (const escapedStr& that) { return s <  that.s; }  
  
  // Casting 
  // Casting to a string is the same as returning the escaped string
  operator std::string() { return escape(); }
  
  // Self-testing code for the escapedStr class.
  static void selfTest();
}; // class escapedStr

// Base class of classes that manage the registration functionality of different modules that may be linked
// into a given executable. Since Sight is a framework its exact functionality depends on the set of widgets
// used with it. The mechanism to select the widgets used in a given scenario is linking. The object files
// of a given set of widgets are linked together into executables such as a user application, slayout or 
// hier_merge. It is the choice of object files that are linked that determines the functionality of a given
// executable. In many cases we need the linked widgets to provide callbacks to perform widget-specific 
// actions such as merging or laying out a type of tag or performing some type of attrValue comparison.
// The choice of functionality is specified in a string and we need the widget that implements this functionality
// to register a callback with this string to make it possible to invoke the functionality. Since the choice
// of widgets is made at link-time and the order in which the widgets' object files are initialized and loaded
// is unspecified, this is difficult to manage. 
//
// The LoadTimeRegistry class implements support for registering call-back functions for the widgets that
// are linked within a given executable. For each task-specific class that inherits from it, the LoadTimeRegistry
// constructor that it initialization code is invoked exactly once, using environment variables as a type of
// mutex. To make sure this environment-based initialization state does not leak from a process to its children,
// LoadTimeRegistry provides functionality to temporarily remove its state from the environment.
// 
// LoadTimeRegistry is used as follows:
// - We derive class X from LoadTimeRegistry. There is one such class for every major functionality (e.g. merging, parsing)
//   - Implement X::name(), which returns a unique string
//   - Implement X::init(), which initializes any data structures (static to X) needed to register the given functionality 
//     within loaded widgets. X::init() is guaranteed to be called once within an executable
// - Derive from X class Y. There is one Y for each widget that implements the given functionality type
//   - Within the constructor of Y register this widget's functionality with the data structures in X. It can be
//     assumed that X::init() has already executed.
//   - Create one static instance of Y within each widget.
class LoadTimeRegistry {
  // The names of all the LoadTimeRegistry's derived classes that have already been initialized.
  static std::set<std::string>* initialized;

  public:

  // Type of the class-specific initialization method that is passed into the base LoadTimeRegistry constructor.
  // It is called exactly once for each class that derives from LoadTimeRegistry to initialize its static data structures.
  // We don't use virtual methods for this because they don't work inside constructors.
  typedef void (*initFunc)();

  // name - Unique string name of the class that derives from LoadTimeRegistry
  // init - Function that is called to initialize this class
  LoadTimeRegistry(std::string name, initFunc init);

  /*// Unique string name of the class that derives from LoadTimeRegistry
  virtual std::string name() const { assert(0); return ""; }
  // Called exactly once for each class that derives from LoadTimeRegistry to initialize its static data structures.
  virtual void init() { assert(0); }*/
  static void init() {}

  // Removes all the environment variables that record the current mutexes of LoadTimeRegistry
  static void liftMutexes();

  // Restores all the environment variables previously removed by liftMutexes
  static void restoreMutexes();
}; // class LoadTimeRegistry

// Create an instance of LoadTimeRegistry to ensure that it is initialized even if it is never derived from
extern LoadTimeRegistry LoadTimeRegInstance;

// Specialization of the LoadTimeRegistry focused on the specific case of parsing structure/configuration files where we need
// to invoke special methods when tags are entered and exited. This registry provides methods to register callback
// functions that will be invoked on tag entry and exit. The entry functions are assumed to take in a properties
// object that describes the tag and returns an object that represents it. This object is placed onto a stack
// (tag entries/exits are assumed to be strictly hierarchical) and passed to the corresponding exit function when 
// its tag is exited.
// The entry functions are assumed to have the prototype:
//    objectType* entry(properties props)
// The exit functions are assumed to have the prototype:
//    void exit(objType* obj)
// Entry functions may return NULL, in which case the corresponding exit function is also passed NULL.
template<typename objType>
class TagFileReaderRegistry: public LoadTimeRegistry {
  public:
  typedef objType* (*enterFunc)(properties::iterator props);
  typedef void (*exitFunc)(objType*);

  // Map the names of tags to the functions to be called when these tags are entered/exited
//  static std::map<std::string, enterFunc>* enterHandlers;
//  static std::map<std::string, exitFunc>*  exitHandlers;

  TagFileReaderRegistry(std::string name);
  // Called exactly once for each class that derives from LoadTimeRegistry to initialize its static data structures.
  static void init();

  protected:
  // The stack of pointers to objects that encode the tags that are currently entered but not exited
//  static std::map<std::string, std::list<objType*> > stack;
  static boost::thread_specific_ptr< std::map<std::string, std::list<objType*> > > stackInstance;

  static boost::thread_specific_ptr< std::map<std::string, enterFunc> > enterHandlersInstance;
  static boost::thread_specific_ptr< std::map<std::string, exitFunc> > exitHandlersInstance;

  static boost::thread_specific_ptr< std::map<std::string, std::list<objType*> > >& getStackInstance(){
    if(!stackInstance.get()){
        stackInstance.reset(new std::map<std::string, std::list<objType*> >);
    }
      return stackInstance;
  }

  static boost::thread_specific_ptr< std::map<std::string, enterFunc> >& getEnterHandlers(){
      if(!enterHandlersInstance.get()){
        enterHandlersInstance.reset(new std::map<std::string, enterFunc>());
      }
      return enterHandlersInstance;
  }

  static boost::thread_specific_ptr< std::map<std::string, exitFunc> >& getExitHandlers(){
        if(!exitHandlersInstance.get()){
            exitHandlersInstance.reset(new std::map<std::string, exitFunc>());
        }
        return exitHandlersInstance;
  }

public:
  // Call the entry handler of the most recently-entered object with name objName
  // and push the object it returns onto the stack.
  static void enter(std::string objName, properties::iterator iter);

  // Call the exit handler of the most recently-entered object with name objName
  // and pop the object off its stack.
  static void exit(std::string objName);

  // Default entry/exit handlers to use when no special handling is needed
  static objType* defaultEnterFunc(properties::iterator props) { return NULL; }
  static void  defaultExitFunc(objType* obj) {}
  
  static std::string str();
}; // class TagFileReaderRegistry

/***********************************************
 ***** Syntactic Sugar for Data Structures *****
 ***********************************************/

// Syntactic sugar for specifying lists
template<class T>
class easylist : public std::list<T> {
  public:
  easylist() {}

  easylist(const T& p0)
  { std::list<T>::push_back(p0); }

  easylist(const T& p0, const T& p1)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); }

  easylist(const T& p0, const T& p1, const T& p2)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); std::list<T>::push_back(p5); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); std::list<T>::push_back(p5); std::list<T>::push_back(p6); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); std::list<T>::push_back(p5); std::list<T>::push_back(p6); std::list<T>::push_back(p7); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); std::list<T>::push_back(p5); std::list<T>::push_back(p6); std::list<T>::push_back(p7); std::list<T>::push_back(p8); }

  easylist(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8, const T& p9)
  { std::list<T>::push_back(p0); std::list<T>::push_back(p1); std::list<T>::push_back(p2); std::list<T>::push_back(p3); std::list<T>::push_back(p4); std::list<T>::push_back(p5); std::list<T>::push_back(p6); std::list<T>::push_back(p7); std::list<T>::push_back(p8); std::list<T>::push_back(p8); std::list<T>::push_back(p9); }
}; // class easylist

// Syntactic sugar for specifying vectors
template<class T>
class easyvector : public std::vector<T> {
  public:
  easyvector() {}

  easyvector(const T& p0)
  { std::vector<T>::push_back(p0); }

  easyvector(const T& p0, const T& p1)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); }

  easyvector(const T& p0, const T& p1, const T& p2)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); std::vector<T>::push_back(p5); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); std::vector<T>::push_back(p5); std::vector<T>::push_back(p6); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); std::vector<T>::push_back(p5); std::vector<T>::push_back(p6); std::vector<T>::push_back(p7); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); std::vector<T>::push_back(p5); std::vector<T>::push_back(p6); std::vector<T>::push_back(p7); std::vector<T>::push_back(p8); }

  easyvector(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8, const T& p9)
  { std::vector<T>::push_back(p0); std::vector<T>::push_back(p1); std::vector<T>::push_back(p2); std::vector<T>::push_back(p3); std::vector<T>::push_back(p4); std::vector<T>::push_back(p5); std::vector<T>::push_back(p6); std::vector<T>::push_back(p7); std::vector<T>::push_back(p8); std::vector<T>::push_back(p8); std::vector<T>::push_back(p9); }
}; // class easyvector


// Syntactic sugar for specifying maps
template<class KeyT, class ValT>
class easymap: public std::map<KeyT, ValT> {
  public:
  easymap() {} 
	
  easymap(const KeyT& key0, const ValT& val0)
  { (*this)[key0] = val0; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1)
  { (*this)[key0] = val0; (*this)[key1] = val1; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4, const KeyT& key5, const ValT& val5)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; (*this)[key5] = val5; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4, const KeyT& key5, const ValT& val5, const KeyT& key6, const ValT& val6)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; (*this)[key5] = val5; (*this)[key6] = val6; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4, const KeyT& key5, const ValT& val5, const KeyT& key6, const ValT& val6, const KeyT& key7, const ValT& val7)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; (*this)[key5] = val5; (*this)[key6] = val6; (*this)[key7] = val7; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4, const KeyT& key5, const ValT& val5, const KeyT& key6, const ValT& val6, const KeyT& key7, const ValT& val7, const KeyT& key8, const ValT& val8)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; (*this)[key5] = val5; (*this)[key6] = val6; (*this)[key7] = val7; (*this)[key8] = val8; }
  
  easymap(const KeyT& key0, const ValT& val0, const KeyT& key1, const ValT& val1, const KeyT& key2, const ValT& val2, const KeyT& key3, const ValT& val3, const KeyT& key4, const ValT& val4, const KeyT& key5, const ValT& val5, const KeyT& key6, const ValT& val6, const KeyT& key7, const ValT& val7, const KeyT& key8, const ValT& val8, const KeyT& key9, const ValT& val9)
  { (*this)[key0] = val0; (*this)[key1] = val1; (*this)[key2] = val2; (*this)[key3] = val3; (*this)[key4] = val4; (*this)[key5] = val5; (*this)[key6] = val6; (*this)[key7] = val7; (*this)[key8] = val8; (*this)[key9] = val9; }
}; // easymap

// Syntactic sugar for specifying sets
template<class T>
class easyset : public std::set<T> {
  public:
  easyset() {}

  easyset(const T& p0)
  { std::set<T>::insert(p0); }

  easyset(const T& p0, const T& p1)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); }

  easyset(const T& p0, const T& p1, const T& p2)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); std::set<T>::insert(p5); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); std::set<T>::insert(p5); std::set<T>::insert(p6); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); std::set<T>::insert(p5); std::set<T>::insert(p6); std::set<T>::insert(p7); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); std::set<T>::insert(p5); std::set<T>::insert(p6); std::set<T>::insert(p7); std::set<T>::insert(p8); }

  easyset(const T& p0, const T& p1, const T& p2, const T& p3, const T& p4, const T& p5, const T& p6, const T& p7, const T& p8, const T& p9)
  { std::set<T>::insert(p0); std::set<T>::insert(p1); std::set<T>::insert(p2); std::set<T>::insert(p3); std::set<T>::insert(p4); std::set<T>::insert(p5); std::set<T>::insert(p6); std::set<T>::insert(p7); std::set<T>::insert(p8); std::set<T>::insert(p8); std::set<T>::insert(p9); }
}; // class easyset

/*******************************
 ***** Configuration Files *****
 *******************************/

/* The behavior of Sight widgets can be parameterized by specifying a configuration file.
 * This file has the same format as the structure file that is laid out by the layout layer,
 * and contains tags that describe the different types of widgets that may exist at runtime 
 * and the properties of these widgets. Since each widget may inherit from another widgets,
 * tags in the configuration file (must like those in the structure file) explicitly document
 * their inheritance hierarchy:
 * [kulfiModule ...][|compModule ...][|module ...][|block ...] ... [/kulfiModule]
 * This gets mapped to a properties object with separate key->value maps for each widget
 * in the inheritance hierarchy. To incorporate a tag in the configuration file into the state
 * of a Sight widget the following steps must be taken:
 * - Register a callback function under the widget's name that takes this properties object
 * - When a tag is read the callback function for its most derived widget name is invoked
 * - This callback creates a Configuration object that is specific to the widget and derives
 *   from a Configuration of the widget from which it is derived (ex: KulfiConfiguration derives
 *   from CompModuleConfiguration, which derives from ModuleConfiguration because kulfiModule 
 *   derives from compModule, which derives from module)
 * - The constructor of each Configuration object takes in properties iterator object that corresponds
 *   to the read tag, incorporates the information into the widget's state (likely some static
 *   data structures specific to the widget) and passes the successor of this iterator to
 *   its parent constructor (this successor's properties must correspond to the parent's widget type)
 *
 * Each widget provides its own scheme for mapping the widgets specified in the configuration
 * file to the widgets that occur at runtime. For example, the configuration file may have
 * several module tags with different labels. In this case the modules widget may record a 
 * global mapping from these names to their properties and whenever the user creates a module
 * with a name that matches one that appeared in the configuration file, the corresponding
 * properties object will be communicated to it. Thus, the constructor of each module will get
 * two properties objects. First, the properties of the widget instance that will be written
 * to the structure file and second, the properties iterator for the configuratoin that are 
 * being applied to this widget. Both will be propagated up the constructor, with the configuration
 * properties iterator advancing each time the parent constructor is called.
 */

class Configuration {
  protected:
  Configuration(properties::iterator props) {}
}; // class Configuration

/* Maps the names of various tags that may appear in a configuration file to the functions
 * that decode them.
 * An entry handler is called when the object's entry tag is encountered in the configuration file. It
 *   takes as input the properties of this tag and returns a pointer to Configuration object that 
 *   decodes it. NULL is a valid return value.
 * An exit handler is called when the object's exit tag is encountered and takes as input a pointer
 *  to its corresponding Configuration object, as returned by the entry function.
 * It is assumed that all objects are hierarchically scoped, in that objects are exited in the
 *   reverse order of their entry. The layout engine keeps track of the entry/exit stacks and
 *   ensures that the appropriate object pointers are passed to exit handlers.
 */
class confHandlerInstantiator : public TagFileReaderRegistry<Configuration> {
  public:
  confHandlerInstantiator() : TagFileReaderRegistry<Configuration>("confHandlerInstantiator") {}
};
class sightConfHandlerInstantiator : confHandlerInstantiator {
  public:
  sightConfHandlerInstantiator();
};
extern sightConfHandlerInstantiator sightConfHandlerInstantance;

// Loads the configuration file(s) stored in the files specified in the given environment variables
typedef easylist<std::string> configFileEnvVars;
void loadSightConfig(const std::list<std::string>& cfgFNameEnv); 

// Given a parser that reads a given configuration file, load it
void loadConfiguration(structureParser& parser);

} // namespace common
} // namespace sight