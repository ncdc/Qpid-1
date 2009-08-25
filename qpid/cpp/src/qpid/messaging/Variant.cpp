/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#include "qpid/messaging/Variant.h"
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

namespace qpid {
namespace client {
}

namespace messaging {

InvalidConversion::InvalidConversion(const std::string& msg) : Exception(msg) {}


namespace {
std::string EMPTY;
}

class VariantImpl
{
  public:
    VariantImpl();
    VariantImpl(bool);
    VariantImpl(uint8_t);
    VariantImpl(uint16_t);
    VariantImpl(uint32_t);
    VariantImpl(uint64_t);
    VariantImpl(int8_t);
    VariantImpl(int16_t);
    VariantImpl(int32_t);
    VariantImpl(int64_t);
    VariantImpl(float);
    VariantImpl(double);
    VariantImpl(const std::string&);
    VariantImpl(const Variant::Map&);
    VariantImpl(const Variant::List&);
    ~VariantImpl();

    VariantType getType() const;

    bool asBool() const;
    uint8_t asUint8() const;
    uint16_t asUint16() const;
    uint32_t asUint32() const;
    uint64_t asUint64() const;
    int8_t asInt8() const;
    int16_t asInt16() const;
    int32_t asInt32() const;
    int64_t asInt64() const;
    float asFloat() const;
    double asDouble() const;
    std::string asString() const;

    const Variant::Map& asMap() const;
    Variant::Map& asMap();
    const Variant::List& asList() const;
    Variant::List& asList();

    const std::string& getString() const;
    std::string& getString();

    void setEncoding(const std::string&);
    const std::string& getEncoding() const;

    static VariantImpl* create(const Variant&);    
  private:
    const VariantType type;
    union {
        bool b;
        uint8_t ui8;
        uint16_t ui16;
        uint32_t ui32;
        uint64_t ui64;
        int8_t i8;
        int16_t i16;
        int32_t i32;
        int64_t i64;
        float f;
        double d;
        void* v;//variable width data
    } value;
    std::string encoding;//optional encoding for variable length data

    std::string getTypeName(VariantType type) const;
    template<class T> T convertFromString() const
    {
        std::string* s = reinterpret_cast<std::string*>(value.v);
        try {
            return boost::lexical_cast<T>(*s);
        } catch(const boost::bad_lexical_cast&) {
            throw InvalidConversion(QPID_MSG("Cannot convert " << *s));
        }
    }
};


VariantImpl::VariantImpl() : type(VOID) { value.i64 = 0; }
VariantImpl::VariantImpl(bool b) : type(BOOL) { value.b = b; }
VariantImpl::VariantImpl(uint8_t i) : type(UINT8) { value.ui8 = i; }
VariantImpl::VariantImpl(uint16_t i) : type(UINT16) { value.ui16 = i; }
VariantImpl::VariantImpl(uint32_t i) : type(UINT32) { value.ui32 = i; }
VariantImpl::VariantImpl(uint64_t i) : type(UINT64) { value.ui64 = i; }
VariantImpl::VariantImpl(int8_t i) : type(INT8) { value.i8 = i; }
VariantImpl::VariantImpl(int16_t i) : type(INT16) { value.i16 = i; }
VariantImpl::VariantImpl(int32_t i) : type(INT32) { value.i32 = i; }
VariantImpl::VariantImpl(int64_t i) : type(INT64) { value.i64 = i; }
VariantImpl::VariantImpl(float f) : type(FLOAT) { value.f = f; }
VariantImpl::VariantImpl(double d) : type(DOUBLE) { value.d = d; }
VariantImpl::VariantImpl(const std::string& s) : type(STRING) { value.v = new std::string(s); }
VariantImpl::VariantImpl(const Variant::Map& m) : type(MAP) { value.v = new Variant::Map(m); }
VariantImpl::VariantImpl(const Variant::List& l) : type(LIST) { value.v = new Variant::List(l); }

VariantImpl::~VariantImpl() { 
    switch (type) {
      case STRING:
        delete reinterpret_cast<std::string*>(value.v);
        break;
      case MAP:
        delete reinterpret_cast<Variant::Map*>(value.v);
        break;
      case LIST:
        delete reinterpret_cast<Variant::List*>(value.v);
        break;
      default:
        break;
    }
}

VariantType VariantImpl::getType() const { return type; }

namespace {

bool same_char(char a, char b) 
{
    return toupper(a) == toupper(b);
}

bool caseInsensitiveMatch(const std::string& a, const std::string& b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), &same_char);
}

const std::string TRUE("True");
const std::string FALSE("False");

bool toBool(const std::string& s)
{
    if (caseInsensitiveMatch(s, TRUE)) return true;
    if (caseInsensitiveMatch(s, FALSE)) return false;
    try { return boost::lexical_cast<int>(s); } catch(const boost::bad_lexical_cast&) {}
    throw InvalidConversion(QPID_MSG("Cannot convert " << s << " to bool"));    
}

}

bool VariantImpl::asBool() const
{
    switch(type) {
      case VOID: return false;
      case BOOL: return value.b;
      case UINT8: return value.ui8;
      case UINT16: return value.ui16;
      case UINT32: return value.ui32;
      case UINT64: return value.ui64;
      case INT8: return value.i8;
      case INT16: return value.i16;
      case INT32: return value.i32;
      case INT64: return value.i64;
      case STRING: return toBool(*reinterpret_cast<std::string*>(value.v));
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(BOOL)));
    }
}
uint8_t VariantImpl::asUint8() const
{
    switch(type) {
      case UINT8: return value.ui8;
      case STRING: return convertFromString<uint8_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(UINT8)));
    }
}
uint16_t VariantImpl::asUint16() const
{
    switch(type) {
      case UINT8: return value.ui8;
      case UINT16: return value.ui16;
      case STRING: return convertFromString<uint16_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(UINT16)));
    }
}
uint32_t VariantImpl::asUint32() const
{
    switch(type) {
      case UINT8: return value.ui8;
      case UINT16: return value.ui16;
      case UINT32: return value.ui32;
      case STRING: return convertFromString<uint32_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(UINT32)));
    }
}
uint64_t VariantImpl::asUint64() const
{
    switch(type) {
      case UINT8: return value.ui8;
      case UINT16: return value.ui16;
      case UINT32: return value.ui32;
      case UINT64: return value.ui64;
      case STRING: return convertFromString<uint64_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(UINT64)));
    }
}
int8_t VariantImpl::asInt8() const
{
    switch(type) {
      case INT8: return value.i8;
      case STRING: return convertFromString<int8_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(INT8)));
    }
}
int16_t VariantImpl::asInt16() const
{
    switch(type) {
      case INT8: return value.i8;
      case INT16: return value.i16;
      case STRING: return convertFromString<int16_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(INT16)));
    }
}
int32_t VariantImpl::asInt32() const
{
    switch(type) {
      case INT8: return value.i8;
      case INT16: return value.i16;
      case INT32: return value.i32;
      case STRING: return convertFromString<int32_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(INT32)));
    }
}
int64_t VariantImpl::asInt64() const
{
    switch(type) {
      case INT8: return value.i8;
      case INT16: return value.i16;
      case INT32: return value.i32;
      case INT64: return value.i64;
      case STRING: return convertFromString<int64_t>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(INT64)));
    }
}
float VariantImpl::asFloat() const
{
    switch(type) {
      case FLOAT: return value.f;
      case STRING: return convertFromString<float>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(FLOAT)));
    }
}
double VariantImpl::asDouble() const
{
    switch(type) {
      case FLOAT: return value.f;
      case DOUBLE: return value.d;
      case STRING: return convertFromString<double>();
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(DOUBLE)));
    }
}
std::string VariantImpl::asString() const
{
    switch(type) {
      case VOID: return EMPTY;
      case BOOL: return value.b ? TRUE : FALSE;
      case UINT8: return boost::lexical_cast<std::string>((int) value.ui8);
      case UINT16: return boost::lexical_cast<std::string>(value.ui16);
      case UINT32: return boost::lexical_cast<std::string>(value.ui32);
      case UINT64: return boost::lexical_cast<std::string>(value.ui64);
      case INT8: return boost::lexical_cast<std::string>((int) value.i8);
      case INT16: return boost::lexical_cast<std::string>(value.i16);
      case INT32: return boost::lexical_cast<std::string>(value.i32);
      case INT64: return boost::lexical_cast<std::string>(value.i64);
      case DOUBLE: return boost::lexical_cast<std::string>(value.d);
      case FLOAT: return boost::lexical_cast<std::string>(value.f);
      case STRING: return *reinterpret_cast<std::string*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(STRING)));
    }
}

const Variant::Map& VariantImpl::asMap() const
{
    switch(type) {
      case MAP: return *reinterpret_cast<Variant::Map*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(MAP)));
    }
}

Variant::Map& VariantImpl::asMap()
{
    switch(type) {
      case MAP: return *reinterpret_cast<Variant::Map*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(MAP)));
    }
}

const Variant::List& VariantImpl::asList() const
{
    switch(type) {
      case LIST: return *reinterpret_cast<Variant::List*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(LIST)));
    }
}

Variant::List& VariantImpl::asList()
{
    switch(type) {
      case LIST: return *reinterpret_cast<Variant::List*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Cannot convert from " << getTypeName(type) << " to " << getTypeName(LIST)));
    }
}

std::string& VariantImpl::getString()
{
    switch(type) {
      case STRING: return *reinterpret_cast<std::string*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Variant is not a string; use asString() if conversion is required."));
    }
}

const std::string& VariantImpl::getString() const
{
    switch(type) {
      case STRING: return *reinterpret_cast<std::string*>(value.v);
      default: throw InvalidConversion(QPID_MSG("Variant is not a string; use asString() if conversion is required."));
    }
}

void VariantImpl::setEncoding(const std::string& s) { encoding = s; }
const std::string& VariantImpl::getEncoding() const { return encoding; }

std::string VariantImpl::getTypeName(VariantType type) const
{
    switch (type) {
      case VOID: return "void";
      case BOOL: return "bool";
      case UINT8: return "uint8";
      case UINT16: return "uint16";
      case UINT32: return "uint32";
      case UINT64: return "uint64";
      case INT8: return "int8";
      case INT16: return "int16";
      case INT32: return "int32";
      case INT64: return "int64";
      case FLOAT: return "float";
      case DOUBLE: return "double";
      case STRING: return "string";
      case MAP: return "map";
      case LIST: return "list";
    }
    return "<unknown>";//should never happen
}

VariantImpl* VariantImpl::create(const Variant& v) 
{
    switch (v.getType()) {
      case BOOL: return new VariantImpl(v.asBool());
      case UINT8: return new VariantImpl(v.asUint8());
      case UINT16: return new VariantImpl(v.asUint16());
      case UINT32: return new VariantImpl(v.asUint32());
      case UINT64: return new VariantImpl(v.asUint64());
      case INT8: return new VariantImpl(v.asInt8());
      case INT16: return new VariantImpl(v.asInt16());
      case INT32: return new VariantImpl(v.asInt32());
      case INT64: return new VariantImpl(v.asInt64());
      case FLOAT: return new VariantImpl(v.asFloat());
      case DOUBLE: return new VariantImpl(v.asDouble());
      case STRING: return new VariantImpl(v.asString());
      case MAP: return new VariantImpl(v.asMap());
      case LIST: return new VariantImpl(v.asList());
      default: return new VariantImpl();
    }
}

Variant::Variant() : impl(new VariantImpl()) {}
Variant::Variant(bool b) : impl(new VariantImpl(b)) {}
Variant::Variant(uint8_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(uint16_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(uint32_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(uint64_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(int8_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(int16_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(int32_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(int64_t i) : impl(new VariantImpl(i)) {}
Variant::Variant(float f) : impl(new VariantImpl(f)) {}
Variant::Variant(double d) : impl(new VariantImpl(d)) {}
Variant::Variant(const std::string& s) : impl(new VariantImpl(s)) {}
Variant::Variant(const char* s) : impl(new VariantImpl(std::string(s))) {}
Variant::Variant(const Map& m) : impl(new VariantImpl(m)) {}
Variant::Variant(const List& l) : impl(new VariantImpl(l)) {}
Variant::Variant(const Variant& v) : impl(VariantImpl::create(v)) {}

Variant::~Variant() { if (impl) delete impl; }

void Variant::reset()
{
    if (impl) delete impl;
    impl = new VariantImpl();
}


Variant& Variant::operator=(bool b)
{
    if (impl) delete impl;
    impl = new VariantImpl(b);
    return *this;
}

Variant& Variant::operator=(uint8_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(uint16_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(uint32_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(uint64_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}

Variant& Variant::operator=(int8_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(int16_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(int32_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}
Variant& Variant::operator=(int64_t i)
{
    if (impl) delete impl;
    impl = new VariantImpl(i);
    return *this;
}

Variant& Variant::operator=(float f)
{
    if (impl) delete impl;
    impl = new VariantImpl(f);
    return *this;
}
Variant& Variant::operator=(double d)
{
    if (impl) delete impl;
    impl = new VariantImpl(d);
    return *this;
}

Variant& Variant::operator=(const std::string& s)
{
    if (impl) delete impl;
    impl = new VariantImpl(s);
    return *this;
}

Variant& Variant::operator=(const char* s)
{
    if (impl) delete impl;
    impl = new VariantImpl(std::string(s));
    return *this;
}

Variant& Variant::operator=(const Map& m)
{
    if (impl) delete impl;
    impl = new VariantImpl(m);
    return *this;
}

Variant& Variant::operator=(const List& l)
{
    if (impl) delete impl;
    impl = new VariantImpl(l);
    return *this;
}

Variant& Variant::operator=(const Variant& v)
{
    if (impl) delete impl;
    impl = VariantImpl::create(v);
    return *this;
}

VariantType Variant::getType() const { return impl->getType(); }
bool Variant::asBool() const { return impl->asBool(); }
uint8_t Variant::asUint8() const { return impl->asUint8(); }
uint16_t Variant::asUint16() const { return impl->asUint16(); }
uint32_t Variant::asUint32() const { return impl->asUint32(); }
uint64_t Variant::asUint64() const { return impl->asUint64(); }
int8_t Variant::asInt8() const { return impl->asInt8(); }
int16_t Variant::asInt16() const { return impl->asInt16(); }
int32_t Variant::asInt32() const { return impl->asInt32(); }
int64_t Variant::asInt64() const { return impl->asInt64(); }
float Variant::asFloat() const { return impl->asFloat(); }
double Variant::asDouble() const { return impl->asDouble(); }
std::string Variant::asString() const { return impl->asString(); }
const Variant::Map& Variant::asMap() const { return impl->asMap(); }
Variant::Map& Variant::asMap() { return impl->asMap(); }
const Variant::List& Variant::asList() const { return impl->asList(); }
Variant::List& Variant::asList() { return impl->asList(); }
const std::string& Variant::getString() const { return impl->getString(); }
std::string& Variant::getString() { return impl->getString(); }
void Variant::setEncoding(const std::string& s) { impl->setEncoding(s); }
const std::string& Variant::getEncoding() const { return impl->getEncoding(); }

Variant::operator bool() const { return asBool(); }
Variant::operator uint8_t() const { return asUint8(); }
Variant::operator uint16_t() const { return asUint16(); }
Variant::operator uint32_t() const { return asUint32(); }
Variant::operator uint64_t() const { return asUint64(); }
Variant::operator int8_t() const { return asInt8(); }
Variant::operator int16_t() const { return asInt16(); }
Variant::operator int32_t() const { return asInt32(); }
Variant::operator int64_t() const { return asInt64(); }
Variant::operator float() const { return asFloat(); }
Variant::operator double() const { return asDouble(); }
Variant::operator const char*() const { return asString().c_str(); }

std::ostream& operator<<(std::ostream& out, const Variant::Map& map)
{
    for (Variant::Map::const_iterator i = map.begin(); i != map.end(); ++i) {
        if (i != map.begin()) out << ", ";
        out << i->first << ":" << i->second;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Variant::List& list)
{
    for (Variant::List::const_iterator i = list.begin(); i != list.end(); ++i) {
        if (i != list.begin()) out << ", ";
        out << *i;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Variant& value)
{
    switch (value.getType()) {
      case MAP:
        out << "{" << value.asMap() << "}";
        break;
      case LIST:
        out << "[" << value.asList() << "]";
        break;
      case VOID:
        out << "<void>";
        break;
      default:
        out << value.asString();
        break;
    }
    return out;    
}

}} // namespace qpid::messaging
