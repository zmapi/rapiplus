#include "RApiPlus.h"

#include <iostream>

#include <string>
#include <cstring>
#include <unordered_map>

#include "boost/python.hpp"
#include "boost/bimap.hpp"
#include "boost/assign.hpp"

class String {

public:

	String() {
		init_(nullptr, 0);
	}

	String(char const * s) {
		init_(s, strlen(s));
	}

	String(std::string const & s) {
		init_(s.c_str(), s.length());
	}

	String(tsNCharcb const & s) {
		init_(s.pData, s.iDataLen);
	}

	~String() {
		if (data_) {
			delete data_;
		}
		if (rstr_) {
			delete rstr_;
		}
	}

	char const * c_str() const {
		return data_;
	}

	tsNCharcb * r_str() const {
		return rstr_;
	}

	// returns a managed copy
	std::string str() const {
		return std::string(data_, len_);
	}

	// returns a managed copy
	boost::python::str p_str() const {
		return boost::python::str(data_, len_);
	}

	size_t size() const {
		return len_;
	}

private:

	void init_(char const * s_data, size_t s_len) {
		if (s_data && s_len > 0) {
			len_ = s_len;
			char * data = new char[len_ + 1];
			memcpy(data, s_data, len_);
			data[len_] = 0;
			data_ = const_cast<char const *>(data);
		} else {
			data_ = nullptr;
			len_ = 0;
		}
		rstr_ = new tsNCharcb;
		rstr_->pData = const_cast<char*>(data_);
		rstr_->iDataLen = len_;
	}

	tsNCharcb * rstr_;
	char const * data_;
	size_t len_;

};

std::ostream &
operator<<(std::ostream & os, String const& s) {
	return os << s.str();
}

tsNCharcb copy_r_str(tsNCharcb const & s) {
	tsNCharcb res;
	res.iDataLen = s.iDataLen;
	res.pData = new char[s.iDataLen];
	memcpy(res.pData, s.pData, s.iDataLen);
	return res;
}

// tsNCharcb view_as_rstr(std::string const & s) {
// 	tsNCharcb res;
// 	res.pData = const_cast<char*>(s.c_str());
// 	res.iDataLen = s.length();
// 	return res;
// }

// https://stackoverflow.com/a/9690436/1793556
PyObject* create_exception_class(const char* name,
                                 PyObject* baseTypeObj = PyExc_Exception) {
    using std::string;
    namespace bp = boost::python;

    string scopeName = bp::extract<string>(bp::scope().attr("__name__"));
    string qualifiedName0 = scopeName + "." + name;
    char* qualifiedName1 = const_cast<char*>(qualifiedName0.c_str());

    PyObject* typeObj = PyErr_NewException(qualifiedName1, baseTypeObj, 0);
    if(!typeObj) bp::throw_error_already_set();
    bp::scope().attr(name) = bp::handle<>(bp::borrowed(typeObj));
    return typeObj;
}


boost::bimap<RApi::SearchField, std::string> SEARCHFIELD_TO_STR =
boost::assign::list_of<boost::bimap<RApi::SearchField, std::string>::relation>
(RApi::Any, "Any")
(RApi::Exchange, "Exchange")
(RApi::ProductCode, "ProductCode")
(RApi::InstrumentType, "InstrumentType")
(RApi::Ticker, "Ticker")
(RApi::Description, "Description")
(RApi::ExpirationDate, "ExpirationDate")
;


boost::bimap<RApi::SearchOperator, std::string> SEARCHOPERATOR_TO_STR =
boost::assign::list_of<boost::bimap<RApi::SearchOperator, std::string>::relation>
(RApi::Equals, "Equals")
(RApi::Contains, "Contains")
(RApi::StartsWith, "StartsWith")
(RApi::EndsWith, "EndsWith")
;


std::ostream &
operator<<(std::ostream & os, boost::python::object const& o) {
	return os << boost::python::extract<std::string>(boost::python::str(o))();
}

std::ostream &
operator<<(std::ostream & os, boost::python::str const& o) {
	return os << boost::python::extract<std::string>(o)();
}
