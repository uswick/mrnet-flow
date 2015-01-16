#pragma once 
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/make_shared.hpp>

// Wrapper for boost:shared_ptr<Type> that can be used as keys in maps because it wraps comparison 
// operations by forwarding them to the Type's own comparison operations. In contrast, the base 
// boost::shared_ptr uses pointer equality.
template <class Type>
class SharedPtr
{
  //public:
  boost::shared_ptr<Type> ptr;
  
  public:
  SharedPtr() {}
  
  // Wraps a raw pointer of an object that has a dynamic copying method with a comparable shared pointer
  SharedPtr(Type* p) {
    Type* c = dynamic_cast<Type*>(p->copy());
    assert(c);
    ptr = boost::shared_ptr<Type>(c);
  }
    
  // Copy constructor
  SharedPtr(const SharedPtr<Type>& o) : ptr(o.ptr) {}
  
  // Constructor for converting across SharedPtr wrappers of compatible types
  template <class OtherType>
  SharedPtr(const SharedPtr<OtherType>& o) : ptr(boost::static_pointer_cast<Type>(o.ptr)) {}
  
  // Constructor for wrapping boost::shared_ptr with a CompoSharedPtr
  SharedPtr(boost::shared_ptr<Type> ptr) : ptr(ptr) {}
  
  template <class OtherType>
  SharedPtr(boost::shared_ptr<OtherType> ptr) : ptr(boost::static_pointer_cast<Type>(ptr)) {}
  
  template <class OtherType>
  friend class SharedPtr;
  
  SharedPtr& operator=(const SharedPtr& o) { ptr = o.ptr; return *this; }
  // If both ptr and o.ptr are != NULL, use their equality operator
  // If both ptr and o.ptr are == NULL, they are equal
  // If only one is == NULL but the other is not, order the NULL object as < the non-NULL object
  bool operator==(const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) == o; else return false; } else { if(o.get()!=NULL) return false; else return true;  } }
  bool operator< (const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) <  o; else return false; } else { if(o.get()!=NULL) return true;  else return false; } }
  bool operator!=(const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) != o; else return true;  } else { if(o.get()!=NULL) return true;  else return false; } }
  bool operator>=(const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) >= o; else return true;  } else { if(o.get()!=NULL) return false; else return true;  } }
  bool operator<=(const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) <= o; else return false; } else { if(o.get()!=NULL) return true;  else return true;  } }
  bool operator> (const SharedPtr<Type> & o) const { if(ptr.get()!=NULL) { if(o.get()!=NULL) return (*ptr.get()) >  o; else return true;  } else { if(o.get()!=NULL) return false; else return false; } }
  
  Type* get() const { return ptr.get(); }
  const Type* operator->() const { return ptr.get(); }
  Type* operator->() { return ptr.get(); }
  
  operator bool() const { return (bool) ptr.get(); }
  
  //PartPtr operator * () { return ptr; }
/*  // Write a human-readable string representation of this object to the given
  // output stream
  virtual std::ostream& str(std::ostream& out) const 
  { return ptr->str(out); }*/
};

// Returns a new instance of a SharedPtr that refers to an instance of SharedPtr<Type>
// We have created an instance of this function for cases with 0-9 input parameters since that is 
// what boost::make_shared provides when the compiler doesn't support varyadic types. Support for 
// varyadic types is future work.
template <class Type>
SharedPtr<Type> makePtr()
{ return boost::make_shared<Type>(); }

template <class Type, class Arg1>
SharedPtr<Type> makePtr(const Arg1& arg1)
{ return boost::make_shared<Type>(arg1); }

template <class Type, class Arg1, class Arg2>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2)
{ return boost::make_shared<Type>(arg1, arg2); }

template <class Type, class Arg1, class Arg2, class Arg3>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3)
{ return boost::make_shared<Type>(arg1, arg2, arg3); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5, arg6); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5, arg6, arg7); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9&  arg9)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9&  arg9, const Arg10&  arg10)
{ return boost::make_shared<Type>(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10, class Arg11>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9&  arg9, const Arg10&  arg10, const Arg11&  arg11)
{ return boost::shared_ptr<Type>(new Type(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11)); }

template <class Type, class Arg1, class Arg2, class Arg3, class Arg4, class Arg5, class Arg6, class Arg7, class Arg8, class Arg9, class Arg10, class Arg11, class Arg12>
SharedPtr<Type> makePtr(const Arg1& arg1, const Arg2& arg2, const Arg3& arg3, const Arg4& arg4, const Arg5& arg5, const Arg6& arg6, const Arg7& arg7, const Arg8& arg8, const Arg9&  arg9, const Arg10&  arg10, const Arg11&  arg11, const Arg12&  arg12)
{ return boost::shared_ptr<Type>(new Type(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12)); }

// Wrapper for boost::dynamic_pointer_cast for SharedPtr
// Used as: dynamicPtrCast<SomePartImplementation>(objectWithPartPtrType);
template <class TargetType, class SourceType>
SharedPtr<TargetType> dynamicPtrCast(SharedPtr<SourceType> s)
{ 
  if(dynamic_cast<TargetType*>(s.get())) return SharedPtr<TargetType>(s);
  else {
    SharedPtr<TargetType> null;
    return null;
  }
}

template <class TargetType, class SourceType>
const SharedPtr<TargetType> dynamicConstPtrCast(SharedPtr<SourceType> s)
{ 
  if(dynamic_cast<const TargetType*>(s.get())) return SharedPtr<TargetType>(s);
  else {
    SharedPtr<TargetType> null;
    return null;
  }
}

template <class TargetType, class SourceType>
SharedPtr<TargetType> staticPtrCast(const SharedPtr<SourceType> s)
{ 
  if(static_cast<TargetType*>(s.get())) return SharedPtr<TargetType>(s);
  else {
    SharedPtr<TargetType> null;
    return null;
  }
}

template <class TargetType, class SourceType>
const SharedPtr<TargetType> staticConstPtrCast(const SharedPtr<SourceType> s)
{ 
  if(static_cast<const TargetType*>(s.get())) return SharedPtr<TargetType>(s);
  else {
    SharedPtr<TargetType> null;
    return null;
  }
}

// Wrapper for boost::make_shared_from_this.
// Used as: makePtrFromThis(make_shared_from_this());
template <class Type>
SharedPtr<Type> makePtrFromThis(boost::shared_ptr<Type> s)
{ return SharedPtr<Type>(s); }

// Initializes a shared pointer from a raw pointer
template <class Type>
SharedPtr<Type> initPtr(Type* p)
{
  return SharedPtr<Type>(p);
}
