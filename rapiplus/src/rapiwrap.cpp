// #include <boost/python/module.hpp>
// #include <boost/python/def.hpp>
// #include <boost/python/str.hpp>
// #include <boost/python/extract.hpp>
// #include <boost/python/init.hpp>
// #include <boost/python/class.hpp>


#include "Python.h"

#include "boost/python.hpp"

#include "zmq.h"

#include <memory>
#include <iostream>
#include <string>
#include <exception>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <unordered_map>

#include "RApiPlus.h"

#include "utils.hpp"

namespace py = boost::python;

using boost::python::object;
using boost::python::str;
using boost::python::list;
using boost::python::tuple;
using boost::python::dict;
using boost::python::init;
using boost::python::class_;
using boost::python::extract;

using std::cout;
using std::cerr;
using std::ostream;
using std::stringstream;
using std::endl;
using std::runtime_error;
using std::thread;

using std::int8_t;
using std::uint8_t;
using std::int16_t;
using std::uint16_t;
using std::int32_t;
using std::uint32_t;
using std::int64_t;
using std::uint64_t;


/*****************************************************************************/

extern char ** environ;

thread_local int r_errno;


// Using py::import and Boost.Python method call mechanisms backfires when
// destructor runs. It may be that the imported module is already destroyed
// at that point and destructor causes a segmentation fault.
class Pickle {

public:

	explicit Pickle() {
		mod_pickle_ = PyImport_ImportModule("pickle");
		dumps_ = PyObject_GetAttrString(mod_pickle_, "dumps");
	}

	object dumps(object const & o) const {
		PyObject * args = PyTuple_Pack(1, o.ptr());
		PyObject * res = PyObject_Call(dumps_, args, 0);
		Py_DECREF(args);
		return object(py::handle<>(res));
	}

private:

	PyObject * mod_pickle_;
	PyObject * dumps_;

};

Pickle pickle;


// Thread-safe container for holding account information.
class Accounts {

public:

	~Accounts() {
		for (auto & it : data_) {
			delete_account(it.second);
		}
	}

	void put(RApi::AccountInfo const & account) {

		RApi::AccountInfo acc;
		acc.sAccountId = copy_r_str(account.sAccountId);
		acc.sAccountName = copy_r_str(account.sAccountName);
		acc.sFcmId = copy_r_str(account.sFcmId);
		acc.sIbId = copy_r_str(account.sIbId);
		RApi::RmsInfo * rms = new RApi::RmsInfo(*account.pRmsInfo);
		rms->sAlgorithm = copy_r_str(account.pRmsInfo->sAlgorithm);
		rms->sAutoLiquidate = copy_r_str(account.pRmsInfo->sAutoLiquidate);
		rms->sAutoLiquidateCriteria = copy_r_str(account.pRmsInfo->sAutoLiquidateCriteria);
		rms->sCurrency = copy_r_str(account.pRmsInfo->sCurrency);
		rms->sDisableOnAutoLiquidate = copy_r_str(account.pRmsInfo->sDisableOnAutoLiquidate);
		rms->sStatus = copy_r_str(account.pRmsInfo->sStatus);
		acc.pRmsInfo = rms;

		// Is AccountId always unique? If not then it's not a valid key.
		std::string key(account.sAccountId.pData, account.sAccountId.iDataLen);

		std::lock_guard<std::mutex> lk(lock_);
		auto match = data_.find(key);
		if (match != data_.end()) {
			delete_account(match->second);
		}
		data_[key] = acc;
	}

	RApi::AccountInfo & get(std::string const & key) {
		std::lock_guard<std::mutex> lk(lock_);
		return data_[key];
	}

private:

	void delete_account(RApi::AccountInfo & account) {
		delete account.sAccountId.pData;
		delete account.sAccountName.pData;
		delete account.sFcmId.pData;
		delete account.sIbId.pData;
		delete account.pRmsInfo->sAlgorithm.pData;
		delete account.pRmsInfo->sAutoLiquidate.pData;
		delete account.pRmsInfo->sAutoLiquidateCriteria.pData;
		delete account.pRmsInfo->sCurrency.pData;
		delete account.pRmsInfo->sDisableOnAutoLiquidate.pData;
		delete account.pRmsInfo->sStatus.pData;
		delete account.pRmsInfo;
	}

	std::mutex lock_;
	std::unordered_map<std::string, RApi::AccountInfo> data_;

};

Accounts g_accounts;


/******************************** EXCEPTIONS *********************************/

PyObject * py_OmneException;
void translate_err_omne(OmneException const & e) {
	OmneException & e_ = const_cast<OmneException &>(e);
	str s_err(e_.getErrorString());
	PyObject * err_str = PyUnicode_FromString(e_.getErrorString());
	PyObject * err_code = PyLong_FromLong(e_.getErrorCode());
	PyObject * args = PyTuple_Pack(2, err_str, err_code);
	PyErr_SetObject(py_OmneException, args);
	// PyErr_SetString(py_OmneException, e_.getErrorString());
	Py_DECREF(args);
	Py_DECREF(err_str);
	Py_DECREF(err_code);
}

class ZMQException : std::exception {

public:

	ZMQException(std::string const & msg) :
		msg_(msg),
		errnum_(errno) {
		stringstream os;
		os << msg_ << ": " << zmq_strerror(errnum_) << " (" << errnum_ << ")";
		what_ = os.str();
	}

	virtual char const * what() const noexcept { return what_.c_str(); }

private:

	std::string msg_;
	int errnum_;
	std::string what_;
};

PyObject * py_ZMQException;
void translate_err_zmq(ZMQException const & e) {
	PyErr_SetString(py_ZMQException, e.what());
}

/********************************** MACROS ***********************************/

// for quick error checking on rapi methods that don't throw exceptions
#define CR(x) \
	do { \
		if ((x) != OK) { \
			throw OmneException(r_errno); \
		} \
	} while (0)

/*****************************************************************************/

class Publisher {

public:

	Publisher(void * sock) :
		sock_(sock) {
	}

	// includes null terminator to topic
	void publish(char const * topic, object const & o) {
		char * data;
		Py_ssize_t data_len;
		Py_ssize_t topic_len = strlen(topic) + 1;  // include null terminator
		object bytes_obj = pickle.dumps(o);
		if (PyBytes_AsStringAndSize(bytes_obj.ptr(), &data, &data_len) < 0) {
			throw std::runtime_error("error on PyBytes_AsStringAndSize");
		}
		publish(topic, topic_len, data, data_len);
	}

	void publish(char const * topic,
	             size_t topic_len,
	             char const * data,
	             size_t data_len) {
		if (zmq_send(sock_, topic, topic_len, ZMQ_SNDMORE) <= 0) {
			throw ZMQException("error sending a message (first part)");
		}
		if (zmq_send(sock_, data, data_len, 0) <= 0) {
			throw ZMQException("error sending a message (last part)");
		}
	}

protected:

	void add_str(dict & res, const std::string name, const tsNCharcb & s) {
		if (s.pData)
			res[name] = String(s).p_str();
	}
	
	template <typename Number>
	void add_num(dict & res,
	                const std::string name,
	                bool test,
	                Number num) {
		if (test)
			res[name] = num;
	}

	// Sometimes API does not provide sRpCode so can't rely on this ...
	// 
	// template <typename Info>
	// void add_rpcode(dict & res, Info const * data) {
	// 	res["RpCode"] = data->iRpCode;
	// 	add_str(res, "RpCodeString", data->sRpCode);
	// }

private:

	void * sock_;

};


class MyAdmCallbacks : public RApi::AdmCallbacks, public Publisher {

public:

	MyAdmCallbacks(void * pub_sock) : Publisher(pub_sock) {
	}

	virtual int Alert(RApi::AlertInfo *data,
	                  void *context,
	                  int *ecode) {
	    PyGILState_STATE gstate;
	    gstate = PyGILState_Ensure();
	    {
			dict res;
			res["AlertType"] = data->iAlertType;
			res["ConnectionId"] = data->iConnectionId;
			res["RpCode"] = data->iRpCode;
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Message", data->sMessage);
			add_str(res, "Ticker", data->sTicker);
			publish("alert", res);
	    }
	    PyGILState_Release(gstate);
		return OK;
	}

private:

};

class MyCallbacks: public RApi::RCallbacks, public Publisher {

public:

	MyCallbacks(void * pub_sock) : Publisher(pub_sock) {
	}


	virtual int AccountList(RApi::AccountListInfo * data,
	                        void *,
	                        int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["RpCode"] = data->iRpCode;
			list l;
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::AccountInfo * x = data->asAccountInfoArray + i;
				dict d;
				add_accountinfo(d, x);
				l.append(d);
				g_accounts.put(*x);
			}
			res["Accounts"] = l;
			publish("account_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}


	virtual int AccountUpdate(RApi::AccountUpdateInfo * data,
	                          void *,
	                          int *) {
		PyGILState_STATE gstate;
		g_accounts.put(data->oAccount);
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_accountinfo(res, &data->oAccount);
			add_str(res, "Action", data->sAction);
			publish("account_update", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}



	virtual int Alert(RApi::AlertInfo * data,
	                  void *,
	                  int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["AlertType"] = data->iAlertType;
			res["ConnectionId"] = data->iConnectionId;
			res["RpCode"] = data->iRpCode;
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Message", data->sMessage);
			add_str(res, "Ticker", data->sTicker);
			publish("alert", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int AskQuote(RApi::AskInfo * data,
	                     void *,
	                     int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_bidask(res, data);
			publish("ask_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int Bar(RApi::BarInfo * data,
	                void *,
	                int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			add_num(res, "bBuyerAsAggressorVolume", data->bBuyerAsAggressorVolume, data->iBuyerAsAggressorVolume);
			res["CustomSession"] = data->bCustomSession;
			add_num(res, "bSellerAsAggressorVolume", data->bSellerAsAggressorVolume, data->iSellerAsAggressorVolume);
			add_num(res, "bSettlementPrice", data->bSettlementPrice, data->dSettlementPrice);
			res["ClosePrice"] = data->dClosePrice;
			res["HighPrice"] = data->dHighPrice;
			res["LowPrice"] = data->dLowPrice;
			res["OpenPrice"] = data->dOpenPrice;
			res["SpecifiedRange"] = data->dSpecifiedRange;
			res["CloseSsm"] = data->iCloseSsm;
			res["EndSsboe"] = data->iEndSsboe;
			res["EndUsecs"] = data->iEndUsecs;
			res["NumTrades"] = data->iNumTrades;
			res["OpenSsm"] = data->iOpenSsm;
			res["SpecifiedMinutes"] = data->iSpecifiedMinutes;
			res["SpecifiedSeconds"] = data->iSpecifiedSeconds;
			res["SpecifiedTicks"] = data->iSpecifiedTicks;
			res["SpecifiedVolume"] = data->iSpecifiedVolume;
			res["StartSsboe"] = data->iStartSsboe;
			res["StartUsecs"] = data->iStartUsecs;
			res["Type"] = data->iType;
			res["Volume"] = data->iVolume;
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Ticker", data->sTicker);
			add_str(res, "SpecifiedDate", data->sSpecifiedDate);
			publish("bar", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}


	virtual int BarReplay(RApi::BarReplayInfo * data,
	                      void *,
	                      int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["CustomSession"] = data->bCustomSession;
			res["SpecifiedRange"] = data->dSpecifiedRange;
			res["CloseSsm"] = data->iCloseSsm;
			res["EndSsboe"] = data->iEndSsboe;
			res["EndUsecs"] = data->iEndUsecs;
			res["OpenSsm"] = data->iOpenSsm;
			res["RpCode"] = data->iRpCode;
			res["SpecifiedMinutes"] = data->iSpecifiedMinutes;
			res["SpecifiedSeconds"] = data->iSpecifiedSeconds;
			res["SpecifiedTicks"] = data->iSpecifiedTicks;
			res["SpecifiedVolume"] = data->iSpecifiedVolume;
			res["StartSsboe"] = data->iStartSsboe;
			res["StartUsecs"] = data->iStartUsecs;
			res["Type"] = data->iType;
			add_str(res, "EndDate", data->sEndDate);
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "StartDate", data->sStartDate);
			add_str(res, "Ticker", data->sTicker);
			publish("bar_replay", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BestAskQuote(RApi::AskInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_bidask(res, data);
			publish("best_ask_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BestBidAskQuote(RApi::BidInfo * bid_data,
	                            RApi::AskInfo * ask_data,
	                            void *,
	                            int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			dict bid;
			add_bidask(bid, bid_data);
			dict ask;
			add_bidask(ask, ask_data);
			res["BestBid"] = bid;
			res["BestAsk"] = ask;
			publish("best_bid_ask_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BestBidQuote(RApi::BidInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_bidask(res, data);
			publish("best_bid_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}
	virtual int BidQuote(RApi::BidInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_bidask(res, data);
			publish("bid_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BinaryContractList(RApi::BinaryContractListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;

			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;

			list l;

			l = list();
			for (int i = 0; i < data->iExchangeArrayLen; i++) {
				l.append(String(data->pExchangeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Exchanges"] = l;

			l = list();
			for (int i = 0; i < data->iExpirationTimeArrayLen; i++) {
				l.append(String(data->pExpirationTimeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Expirations"] = l;

			l = list();
			for (int i = 0; i < data->iProductCodeArrayLen; i++) {
				l.append(String(data->pProductCodeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["ProductCodes"] = l;

			l = list();
			for (int i = 0; i < data->iBinaryContractTypeArrayLen; i++) {
				l.append(String(data->pBinaryContractTypeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["BinaryContractTypes"] = l;

			l = list();
			for (int i = 0; i < data->iPeriodCodeArrayLen; i++) {
				l.append(String(data->pPeriodCodeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["PeriodCodes"] = l;

			l = list();
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::RefDataInfo * x = data->asRefDataInfoArray + i;
				dict d;
				add_refdata(d, x);
				l.append(d);
			}
			if (py::len(l) > 0) {
				res["RefData"] = l;
			}

			publish("binary_contract_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BracketReplay(RApi::BracketReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("bracket_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BracketTierModify(RApi::BracketTierModifyInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("bracket_tier_modify", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BracketUpdate(RApi::BracketUpdateInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("bracket_update", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int BustReport(RApi::OrderBustReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("bust_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int CancelReport(RApi::OrderCancelReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("cancel_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int CloseMidPrice(RApi::CloseMidPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("close_mid_price", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ClosePrice(RApi::ClosePriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("close_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ClosingIndicator(RApi::ClosingIndicatorInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("closing_indicator", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int Dbo(RApi::DboInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "PreviousPrice", data->bPreviousPrice, data->dPreviousPrice);
			res["Price"] = data->dPrice;
			res["Size"] = data->iSize;
			res["Id"] = (size_t)data->pContext;
			add_str(res, "DboUpdateType", data->sDboUpdateType);
			add_str(res, "ExchOrdId", data->sExchOrdId);
			add_str(res, "Priority", data->sPriority);
			add_str(res, "Side", data->sSide);
			add_detailed_ts(res, data);
			add_baseinfo(res, data);
			publish("dbo", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int DboBookRebuild(RApi::DboBookRebuildInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;
			add_num(res, "Price", data->bPrice, data->dPrice);
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Ticker", data->sTicker);
			publish("dbo_book_rebuild", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int EndQuote(RApi::EndQuoteInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_baseinfo(res, data);
			res["UpdateType"] = data->iUpdateType;
			publish("end_quote", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int EquityOptionStrategyList(RApi::EquityOptionStrategyListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;

			list l;

			l = list();
			for (int i = 0; i < data->iExchangeArrayLen; i++) {
				l.append(String(data->pExchangeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Exchanges"] = l;

			l = list();
			for (int i = 0; i < data->iExpirationArrayLen; i++) {
				l.append(String(data->pExpirationArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Expirations"] = l;

			l = list();
			for (int i = 0; i < data->iUnderlyingArrayLen; i++) {
				l.append(String(data->pUnderlyingArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Underlyings"] = l;

			l = list();
			for (int i = 0; i < data->iStrategyTypeArrayLen; i++) {
				l.append(String(data->pStrategyTypeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["StrategyTypes"] = l;

			l = list();
			for (int i = 0; i < data->iTickerArrayLen; i++) {
				l.append(String(data->pTickerArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Tickers"] = l;

			publish("equity_option_strategy_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ExchangeList(RApi::ExchangeListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			d["Id"] = (size_t)data->pContext;
			d["RpCode"] = data->iRpCode;
			list l;
			for (int i = 0; i < data->iArrayLen; i++) {
				l.append(str(data->asExchangeArray[i].pData,
				             data->asExchangeArray[i].iDataLen));
			}
			d["Exchanges"] = l;
			publish("exchange_list", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ExecutionReplay(RApi::ExecutionReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("execution_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int FailureReport(RApi::OrderFailureReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("failure_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int FillReport(RApi::OrderFillReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("fill_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int HighBidPrice(RApi::HighBidPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("high_bid_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int HighPrice(RApi::HighPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("high_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int InstrumentByUnderlying(RApi::InstrumentByUnderlyingInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;

			list l;

			l = list();
			for (int i = 0; i < data->iExchangeArrayLen; i++) {
				l.append(String(data->pExchangeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Exchanges"] = l;

			l = list();
			for (int i = 0; i < data->iExpirationArrayLen; i++) {
				l.append(String(data->pExpirationArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Expirations"] = l;

			l = list();
			for (int i = 0; i < data->iUnderlyingArrayLen; i++) {
				l.append(String(data->pUnderlyingArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Underlyings"] = l;

			l = list();
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::RefDataInfo * x = data->asRefDataInfoArray + i;
				dict d;
				add_refdata(d, x);
				l.append(d);
			}
			if (py::len(l) > 0) {
				res["RefData"] = l;
			}

			publish("instrument_by_underlying", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int InstrumentSearch(RApi::InstrumentSearchInfo * data,
	                             void *,
	                             int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;
			list l;
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::RefDataInfo * x = data->asArray + i;
				dict d;
				add_refdata(d, x);
				l.append(d);
			}
			res["Results"] = l;
			publish("instrument_search", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int LimitOrderBook(RApi::LimitOrderBookInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			list l;
			for (int i = 0; i < data->iBidArrayLen; i++) {
				dict d;
				d["Price"] = data->adBidPriceArray[i];
				d["Size"] = data->aiBidSizeArray[i];
				d["ImpliedSize"] = data->aiBidImpliedSizeArray[i];
				d["NumOrders"] = data->aiBidNumOrdersArray[i];
				d["Ssboe"] = data->aiBidSsboeArray[i];
				d["Usecs"] = data->aiBidUsecsArray[i];
				l.append(d);
			}
			res["Bids"] = l;
			l = list();
			for (int i = 0; i < data->iAskArrayLen; i++) {
				dict d;
				d["Price"] = data->adAskPriceArray[i];
				d["Size"] = data->aiAskSizeArray[i];
				d["ImpliedSize"] = data->aiAskImpliedSizeArray[i];
				d["NumOrders"] = data->aiAskNumOrdersArray[i];
				d["Ssboe"] = data->aiAskSsboeArray[i];
				d["Usecs"] = data->aiAskUsecsArray[i];
				l.append(d);
			}
			res["Asks"] = l;
			res["RpCode"] = data->iRpCode;
			add_baseinfo(res, data);
			publish("limit_order_book", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int LineUpdate(RApi::LineInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("line_update", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int LowAskPrice(RApi::LowAskPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("low_ask_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int LowPrice(RApi::LowPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("low_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int MarketMode(RApi::MarketModeInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_str(res, "Event", data->sEvent);
			add_str(res, "MarketMode", data->sMarketMode);
			add_str(res, "Reason", data->sReason);
			add_detailed_ts(res, data);
			add_baseinfo(res, data);
			publish("market_mode", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int MidPrice(RApi::MidPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_baseinfo(res, data);
			add_num(res, "HighPrice", data->bHighPrice, data->dHighPrice);
			add_num(res, "LastPrice", data->bLastPrice, data->dLastPrice);
			add_num(res, "LowPrice", data->bLowPrice, data->dLowPrice);
			add_num(res, "NetChange", data->bNetChange, data->dNetChange);
			add_num(res, "OpenPrice", data->bOpenPrice, data->dOpenPrice);
			add_num(res, "PercentChange", data->bPercentChange, data->dPercentChange);
			publish("mid_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ModifyReport(RApi::OrderModifyReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("modify_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int NotCancelledReport(RApi::OrderNotCancelledReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("not_cancelled_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int NotModifiedReport(RApi::OrderNotModifiedReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("not_modified_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OpeningIndicator(RApi::OpeningIndicatorInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("opening_indicator", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}
	virtual int OpenInterest(RApi::OpenInterestInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Quantity", data->bQuantityFlag, data->iQuantity);
			add_baseinfo(res, data);
			publish("open_interest", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OpenOrderReplay(RApi::OrderReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("open_order_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OpenPrice(RApi::OpenPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_baseinfo(res, data);
			publish("open_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OptionList(RApi::OptionListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;

			list l;

			l = list();
			for (int i = 0; i < data->iExchangeArrayLen; i++) {
				l.append(String(data->pExchangeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Exchanges"] = l;

			l = list();
			for (int i = 0; i < data->iExpirationCCYYMMArrayLen; i++) {
				l.append(String(data->pExpirationCCYYMMArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Expirations"] = l;

			l = list();
			for (int i = 0; i < data->iProductCodeArrayLen; i++) {
				l.append(String(data->pProductCodeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["ProductCodes"] = l;

			l = list();
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::RefDataInfo * x = data->asRefDataInfoArray + i;
				dict d;
				add_refdata(d, x);
				l.append(d);
			}
			if (py::len(l) > 0) {
				res["RefData"] = l;
			}

			publish("option_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OrderHistoryDates(RApi::OrderHistoryDatesInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("order_history_dates", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OrderReplay(RApi::OrderReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("order_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int OtherReport(RApi::OrderReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("other_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int PasswordChange(RApi::PasswordChangeInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("password_change", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int PnlReplay(RApi::PnlReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("pnl_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int PnlUpdate(RApi::PnlInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("pnl_update", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int PriceIncrUpdate(RApi::PriceIncrInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["RpCode"] = data->iRpCode;
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Ticker", data->sTicker);
			list l;
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::PriceIncrRow * x = data->asPriceIncrArray + i;
				dict d;
				d["FirstPrice"] = x->dFirstPrice;
				d["LastPrice"] = x->dLastPrice;
				d["PriceIncr"] = x->dPriceIncr;
				d["FirstOperator"] = x->iFirstOperator;
				d["LastOperator"] = x->iLastOperator;
				d["Precision"] = x->iPrecision;
				l.append(d);
			}
			res["Increments"] = l;
			publish("price_incr_update", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int ProductRmsList(RApi::ProductRmsListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("product_rms_list", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int Quote(RApi::QuoteReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "FillSize", data->bFill, data->iFillSize);
			add_num(res, "FillPrice", data->bFill, data->dFillPrice);
			add_num(res, "AskPriceToFill", data->bAsk, data->dAskPriceToFill);
			add_num(res, "AskQuantityToFill", data->bAsk, data->iAskQuantityToFill);
			add_num(res, "BidPriceToFill", data->bBid, data->dBidPriceToFill);
			add_num(res, "BidQuantityToFill", data->bBid, data->iBidQuantityToFill);
			add_baseinfo(res, data);
			res["ConnId"] = data->iConnId;
			res["SourceNsecs"] = data->iSourceNsecs;
			res["SourceSsboe"] = data->iSourceSsboe;
			dict d;
			add_str(res, "AccountId", data->oAccount.sAccountId);
			res["Account"] = d;
			add_str(res, "FillSide", data->sFillSide);
			add_str(res, "QuoteId", data->sQuoteId);
			add_str(res, "QuoteMsgId", data->sQuoteMsgId);
			add_str(res, "Remarks", data->sRemarks);
			add_str(res, "ReportId", data->sReportId);
			add_str(res, "ReportType", data->sReportType);
			add_str(res, "User", data->sUser);
			add_str(res, "UserMsg", data->sUserMsg);
			publish("quote_report", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int Quote(RApi::QuoteInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "AskPriceToFill", data->bAsk, data->dAskPriceToFill);
			add_num(res, "AskQuantityToFill", data->bAsk, data->iAskQuantityToFill);
			add_num(res, "BidPriceToFill", data->bBid, data->dBidPriceToFill);
			add_num(res, "BidQuantityToFill", data->bBid, data->iBidQuantityToFill);
			add_baseinfo(res, data);
			dict d;
			add_str(res, "AccountId", data->oAccount.sAccountId);
			res["Account"] = d;
			add_str(res, "QuoteId", data->sQuoteId);
			add_str(res, "QuoteMsgId", data->sQuoteMsgId);
			add_str(res, "Status", data->sStatus);
			add_str(res, "Text", data->sText);
			add_str(res, "User", data->sUser);
			add_str(res, "UserMsg", data->sUserMsg);
			publish("quote_info", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int QuoteReplay(RApi::QuoteReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["RpCode"] = data->iRpCode;
			res["Id"] = (size_t)data->pContext;
			add_str(res, "AccountId", data->oAccount.sAccountId);
			publish("quote_replay", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int RefData(RApi::RefDataInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_refdata(res, data);
			publish("ref_data", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int RejectReport(RApi::OrderRejectReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("reject_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int SettlementPrice(RApi::SettlementPriceInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "Price", data->bPriceFlag, data->dPrice);
			add_str(res, "Date", data->sDate);
			add_str(res, "PriceType", data->sPriceType);
			add_baseinfo(res, data);
			publish("settlement_price", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int SingleOrderReplay(RApi::SingleOrderReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("single_order_replay", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int SodUpdate(RApi::SodReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("sod_update", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int StatusReport(RApi::OrderStatusReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("status_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int Strategy(RApi::StrategyInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;
			add_str(res, "Ticker", data->sTicker);
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Type", data->sType);
			add_str(res, "ProductCode", data->sProductCode);
			list l;
			for (int i = 0; i < data->iLegArrayLen; i++) {
				RApi::StrategyLegInfo * x = data->asLegArray + i;
				dict d;
				d["Ratio"] = x->iRatio;
				add_str(d, "Exchange", x->sExchange);
				add_str(d, "InstrumentType", x->sInstrumentType);
				add_str(d, "ProductCode", x->sProductCode);
				add_str(d, "Ticker", x->sTicker);
				l.append(d);
			}
			res["Legs"] = l;
			publish("strategy", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int StrategyList(RApi::StrategyListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;

			list l;

			l = list();
			for (int i = 0; i < data->iExchangeArrayLen; i++) {
				l.append(String(data->pExchangeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Exchanges"] = l;

			l = list();
			for (int i = 0; i < data->iExpirationCCYYMMArrayLen; i++) {
				l.append(String(data->pExpirationCCYYMMArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Expirations"] = l;

			l = list();
			for (int i = 0; i < data->iProductCodeArrayLen; i++) {
				l.append(String(data->pProductCodeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["ProductCodes"] = l;

			l = list();
			for (int i = 0; i < data->iStrategyTypeArrayLen; i++) {
				l.append(String(data->pStrategyTypeArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["StrategyTypes"] = l;

			l = list();
			for (int i = 0; i < data->iTickerArrayLen; i++) {
				l.append(String(data->pTickerArray[i]).p_str());
			}
			if (py::len(l) > 0)
				res["Tickers"] = l;

			publish("strategy_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeCondition(RApi::TradeInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_tradeinfo(res, data);
			publish("trade_condition", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeCorrectReport(RApi::OrderTradeCorrectReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("trade_correct_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradePrint(RApi::TradeInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_tradeinfo(res, data);
			publish("trade_print", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeReplay(RApi::TradeReplayInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["EndSsboe"] = data->iEndSsboe;
			res["RpCode"] = data->iRpCode;
			res["StartSsboe"] = data->iStartSsboe;
			add_str(res, "Exchange", data->sExchange);
			add_str(res, "Ticker", data->sTicker);
			publish("trade_replay", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeRoute(RApi::TradeRouteInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_traderoute(res, data);
			publish("trade_route", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeRouteList(RApi::TradeRouteListInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			res["Id"] = (size_t)data->pContext;
			res["RpCode"] = data->iRpCode;
			list l;
			for (int i = 0; i < data->iArrayLen; i++) {
				RApi::TradeRouteInfo * x = data->asTradeRouteInfoArray + i;
				dict d;
				add_traderoute(d, x);
				l.append(d);
			}
			res["TradeRoutes"] = l;
			publish("trade_route_list", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TradeVolume(RApi::TradeVolumeInfo * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict res;
			add_num(res, "TotalVolume", data->bTotalVolumeFlag, data->iTotalVolume);
			add_detailed_ts(res, data);
			add_baseinfo(res, data);
			publish("trade_volume", res);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TriggerPulledReport(RApi::OrderTriggerPulledReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("trigger_pulled_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

	virtual int TriggerReport(RApi::OrderTriggerReport * data,
	                         void *,
	                         int *) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();
		{
			dict d;
			publish("trigger_report", d);
		}
		PyGILState_Release(gstate);
		return OK;
	}

private:

	template <typename BidOrAsk>
	void add_bidask(dict & res, BidOrAsk const * data) {
		add_num(res, "LeanPrice", data->bLeanPriceFlag, data->dLeanPrice);
		add_num(res, "Price", data->bPriceFlag, data->dPrice);
		add_num(res, "Size", data->bSizeFlag, data->iSize);
		res["ImpliedSize"] = data->iImpliedSize;
		res["NumOrders"] = data->iNumOrders;
		res["UpdateType"] = data->iUpdateType;
		add_baseinfo(res, data);
	}

	template <typename Info>
	void add_baseinfo(dict & res, Info const * data) {
		res["Ssboe"] = data->iSsboe;
		res["Type"] = data->iType;
		res["Usecs"] = data->iUsecs;
		add_str(res, "Exchange", data->sExchange);
		add_str(res, "Ticker", data->sTicker);
	}

	template <typename Info>
	void add_detailed_ts(dict & res, Info const * data) {
		res["JopNsecs"] = data->iJopNsecs;
		res["JopSsboe"] = data->iJopSsboe;
		res["SourceNsecs"] = data->iSourceNsecs;
		res["SourceSsboe"] = data->iSourceSsboe;
		res["SourceUsecs"] = data->iSourceUsecs;
	}

	void add_refdata(dict & res, RApi::RefDataInfo * data) {
		add_num(res, "CapPrice", data->bCapPrice, data->dCapPrice);
		add_num(res, "FloorPrice", data->bFloorPrice, data->dFloorPrice);
		add_num(res, "MaxPriceVariation", data->bMaxPriceVariation, data->dMaxPriceVariation);
		add_num(res, "SinglePointValue", data->bSinglePointValue, data->dSinglePointValue);
		add_num(res, "StrikePrice", data->bStrikePrice, data->dStrikePrice);
		res["RpCode"] = data->iRpCode;
		res["Ssboe"] = data->iSsboe;
		res["Type"] = data->iType;
		add_str(res, "BinaryContractType", data->sBinaryContractType);
		add_str(res, "Currency", data->sCurrency);
		add_str(res, "Description", data->sDescription);
		add_str(res, "Exchange", data->sExchange);
		add_str(res, "Expiration", data->sExpiration);
		add_str(res, "ExpirationTime", data->sExpirationTime);
		add_str(res, "InstrumentType", data->sInstrumentType);
		add_str(res, "IsTradable", data->sIsTradable);
		add_str(res, "OpenTime", data->sOpenTime);
		add_str(res, "PeriodCode", data->sPeriodCode);
		add_str(res, "ProductCode", data->sProductCode);
		add_str(res, "PutCallIndicator", data->sPutCallIndicator);
		add_str(res, "Ticker", data->sTicker);
		add_str(res, "TradingExchange", data->sTradingExchange);
		add_str(res, "TradingTicker", data->sTradingTicker);
		add_str(res, "Underlying", data->sUnderlying);
	}

	void add_accountinfo(dict & res, RApi::AccountInfo * data) {
		add_str(res, "AccountId", data->sAccountId);
		add_str(res, "AccountName", data->sAccountName);
		add_str(res, "FcmId", data->sFcmId);
		add_str(res, "IbId", data->sIbId);
		RApi::RmsInfo * rms = data->pRmsInfo;
		add_num(res,
				"AutoLiquidateMaxMinAccountBalance",
				rms->bAutoLiquidateMaxMinAccountBalance,
				rms->dAutoLiquidateMaxMinAccountBalance);
		add_num(res,
				"AutoLiquidateThreshold",
				rms->bAutoLiquidateThreshold,
				rms->dAutoLiquidateThreshold);
		add_num(res,
				"MinMarginBalance",
				rms->bMinMarginBalance,
				rms->dMinMarginBalance);
		res["CheckMinAccountBalance"] = rms->bCheckMinAccountBalance;
		res["LossLimit"] = rms->dLossLimit;
		res["iBuyLimit"] = rms->iBuyLimit;
		res["iMaxOrderQty"] = rms->iMaxOrderQty;
		res["iSellLimit"] = rms->iSellLimit;
		add_str(res, "Algorithm", rms->sAlgorithm);
		add_str(res, "AutoLiquidate", rms->sAutoLiquidate);
		add_str(res, "AutoLiquidateCriteria", rms->sAutoLiquidateCriteria);
		add_str(res, "Currency", rms->sCurrency);
		add_str(res, "DisableOnAutoLiquidate", rms->sDisableOnAutoLiquidate);
		add_str(res, "Status", rms->sStatus);
	}

	void add_traderoute(dict & res, RApi::TradeRouteInfo * data) {
		res["Id"] = (size_t)data->pContext;
		res["Type"] = data->iType;
		add_str(res, "Default", data->sDefault);
		add_str(res, "Exchange", data->sExchange);
		add_str(res, "FcmId", data->sFcmId);
		add_str(res, "IbId", data->sIbId);
		add_str(res, "Status", data->sStatus);
		add_str(res, "TradeRoute", data->sTradeRoute);
	}

	void add_tradeinfo(dict & res, RApi::TradeInfo * data) {
		add_num(res, "NetChange", data->bNetChangeFlag, data->dNetChange);
		add_num(res, "PercentChange", data->bPercentChangeFlag, data->dPercentChange);
		add_num(res, "Price", data->bPriceFlag, data->dPrice);
		add_num(res, "VolumeBought", data->bVolumeBoughtFlag, data->iVolumeBought);
		add_num(res, "VolumeSold", data->bVolumeSoldFlag, data->iVolumeSold);
		res["Size"] = data->iSize;
		add_detailed_ts(res, data);
		add_str(res, "AggressorExchOrdId", data->sAggressorExchOrdId);
		add_str(res, "AggressorSide", data->sAggressorSide);
		add_str(res, "Condition", data->sCondition);
		add_str(res, "ExchOrdId", data->sExchOrdId);
		add_baseinfo(res, data);
	}


};

/*****************************************************************************/

class Engine {

public:

	Engine(std::string const & pub_addr) :
		pub_addr_(pub_addr) {
	}

	~Engine() {
		// This hangs for some reason, maybe not necessary when terminating app
		// anyway ...
		// zmq_ctx_term(zctx_);
	}

	void start() {

		if (eng_.get()) {
			throw std::runtime_error("Engine already started");
		}

		zctx_ = zmq_ctx_new();
		if (zmq_ctx_set(zctx_, ZMQ_BLOCKY, 0) != 0) {
			throw ZMQException("error on zmq_ctx_set");
		}
		void * sock_pub = zmq_socket(zctx_, ZMQ_PUB);
		if (zmq_bind(sock_pub, pub_addr_.c_str()) != 0) {
			throw ZMQException("error binding zmq pub socket");
		}

		adm_callbacks_ = std::make_unique<MyAdmCallbacks>(sock_pub);
		callbacks_ = std::make_unique<MyCallbacks>(sock_pub);

		RApi::REngineParams engine_params;
		String sAppName("SampleMD");
		engine_params.sAppName = *sAppName.r_str();
		String sAppVersion("1.0.0.0");
		engine_params.sAppVersion = *sAppVersion.r_str();
		String sAdmCnnctPt("dd_admin_sslc");
		engine_params.sAdmCnnctPt = *sAdmCnnctPt.r_str();
		engine_params.envp = environ;
		engine_params.pAdmCallbacks = adm_callbacks_.get();
		String sLogFilePath("rapi.log");
		engine_params.sLogFilePath = *sLogFilePath.r_str();

		eng_ = std::make_unique<RApi::REngine>(&engine_params);

		RApi::LoginParams login_params;
		login_params.pCallbacks = callbacks_.get();
		String sMdUser = String(std::getenv("RAPI_USERNAME"));
		login_params.sMdUser = *sMdUser.r_str();
		String sMdPassword = String(std::getenv("RAPI_PASSWORD"));
		login_params.sMdPassword = *sMdPassword.r_str();
		String sMdCnnctPt = String(std::getenv("RAPI_MD_ENDPOINT"));
		login_params.sMdCnnctPt = *sMdCnnctPt.r_str();
		String sIhUser = String(std::getenv("RAPI_USERNAME"));
		login_params.sIhUser = *sIhUser.r_str();
		String sIhPassword = String(std::getenv("RAPI_PASSWORD"));
		login_params.sIhPassword = *sIhPassword.r_str();
		String sIhCnnctPt = String(std::getenv("RAPI_IH_ENDPOINT"));
		login_params.sIhCnnctPt = *sIhCnnctPt.r_str();
		String sUser = String(std::getenv("USER"));
		login_params.sUser = *sUser.r_str();
		String sPassword = String(std::getenv("RAPI_PASSWORD"));
		login_params.sPassword = *sPassword.r_str();
		String sTsCnnctPt = String(std::getenv("RAPI_TS_ENDPOINT"));
		login_params.sTsCnnctPt = *sTsCnnctPt.r_str();
		String sPnlCnnctPt = String(std::getenv("RAPI_PNL_ENDPOINT"));
		login_params.sPnlCnnctPt = *sPnlCnnctPt.r_str();

		if (!eng_->login(&login_params, &r_errno)) {
			throw OmneException(r_errno);
		}

	}

	void start_async() {
		thread(&Engine::start, this).detach();
	}

	// cancel_all_orders
	// cancel_order
	// cancel_order_list
	// cancel_quote_list
	// change_password
	// get_order_context
	// link_orders
	// list_order_history_dates
	// modify_order
	// modify_order_list
	// modify_order_ref_data
	// replay_all_orders
	// replay_brackets
	// replay_executions
	// replay_historical_orders
	// replay_open_orders
	// replay_pnl
	// replay_single_historical_order
	// replay_single_order
	// send_bracket_order
	// send_oca_list
	// send_order
	// submit_quote_list
	// subscribe_bracket
	// subscribe_order
	// subscribe_pnl
	// subscribe_trade_route
	// unsubscribe_bracket
	// unsubscribe_order
	// unsubscribe_pnl

	void get_accounts() {
		CR(eng_->getAccounts(&r_errno));
	}

	size_t get_equity_option_strategy_list() {
		return get_equity_option_strategy_list("", "", "", "");
	}

	size_t get_equity_option_strategy_list(std::string const & exchange) {
		return get_equity_option_strategy_list(exchange, "", "", "");
	}

	size_t get_equity_option_strategy_list(std::string const & exchange,
	                                       std::string const & underlying) {
		return get_equity_option_strategy_list(exchange, underlying, "", "");
	}

	size_t get_equity_option_strategy_list(std::string const & exchange,
	                                       std::string const & underlying,
	                                       std::string const & strategy_type) {
		return get_equity_option_strategy_list(exchange,
		                                       underlying,
		                                       strategy_type,
		                                       "");
	}

	size_t get_equity_option_strategy_list(std::string const & exchange,
	                         std::string const & underlying,
	                         std::string const & strategy_type,
	                         std::string const & expiration) {
	    size_t id = id_now_++;
	    String s_exchange(exchange);
	    String s_underlying(underlying);
	    String s_strategy_type(strategy_type);
	    String s_expiration(expiration);
		CR(eng_->getEquityOptionStrategyList(s_exchange.r_str(),
		                                     s_underlying.r_str(),
		                                     s_strategy_type.r_str(),
		                                     s_expiration.r_str(),
		                                     (void*)id,
		                                     &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			get_equity_option_strategy_list_overloads, Engine::get_equity_option_strategy_list, 0, 4);

	size_t get_instrument_by_underlying() {
		return get_instrument_by_underlying("", "", "");
	}

	size_t get_instrument_by_underlying(std::string const & underlying) {
		return get_instrument_by_underlying(underlying, "", "");
	}

	size_t get_instrument_by_underlying(std::string const & underlying,
	                                    std::string const & exchange) {
		return get_instrument_by_underlying(underlying, exchange, "");
	}

	size_t get_instrument_by_underlying(std::string const & underlying,
	                                    std::string const & exchange,
	                                    std::string const & expiration) {
	    size_t id = id_now_++;
	    String s_underlying(underlying);
	    String s_exchange(exchange);
	    String s_expiration(expiration);
		CR(eng_->getInstrumentByUnderlying(s_underlying.r_str(),
		                                   s_exchange.r_str(),
		                                   s_expiration.r_str(),
		                                   (void*)id,
		                                   &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			get_instrument_by_underlying_overloads,
			Engine::get_instrument_by_underlying, 0, 3);


	size_t get_option_list() {
		return get_option_list("", "", "");
	}

	size_t get_option_list(std::string const & exchange) {
		return get_option_list(exchange, "", "");
	}

	size_t get_option_list(std::string const & exchange,
	                       std::string const & product_code) {
		return get_option_list(exchange, product_code, "");
	}

	size_t get_option_list(std::string const & exchange,
	                       std::string const & product_code,
	                       std::string const & expiration) {
	    size_t id = id_now_++;
	    String s_exchange(exchange);
	    String s_product_code(product_code);
	    String s_expiration(expiration);
		CR(eng_->getOptionList(s_exchange.r_str(),
		                       s_product_code.r_str(),
		                       s_expiration.r_str(),
		                       (void*)id,
		                       &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			get_option_list_overloads, Engine::get_option_list, 0, 3);


	void get_price_incr_info(std::string const & exchange,
	                         std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->getPriceIncrInfo(s_exchange.r_str(),
		                          s_ticker.r_str(),
		                          &r_errno));
	}

	void get_ref_data(std::string const & exchange,
	                  std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->getRefData(s_exchange.r_str(), s_ticker.r_str(), &r_errno));
	}

	size_t get_strategy_info(std::string const & exchange,
	                         std::string const & ticker) {
	    size_t id = id_now_++;
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->getStrategyInfo(s_exchange.r_str(),
		                         s_ticker.r_str(),
		                         (void*)id,
		                         &r_errno));
		return id;
	}

	size_t get_strategy_list() {
		return get_strategy_list("", "", "", "");
	}

	size_t get_strategy_list(std::string const & exchange) {
		return get_strategy_list(exchange, "", "", "");
	}

	size_t get_strategy_list(std::string const & exchange,
	                         std::string const & product_code) {
		return get_strategy_list(exchange, product_code, "", "");
	}

	size_t get_strategy_list(std::string const & exchange,
	                         std::string const & product_code,
	                         std::string const & strategy_type) {
		return get_strategy_list(exchange, product_code, strategy_type, "");
	}

	size_t get_strategy_list(std::string const & exchange,
	                         std::string const & product_code,
	                         std::string const & strategy_type,
	                         std::string const & expiration) {
	    size_t id = id_now_++;
	    String s_exchange(exchange);
	    String s_product_code(product_code);
	    String s_strategy_type(strategy_type);
	    String s_expiration(expiration);
		CR(eng_->getStrategyList(s_exchange.r_str(),
		                         s_product_code.r_str(),
		                         s_strategy_type.r_str(),
		                         s_expiration.r_str(),
		                         (void*)id,
		                         &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			get_strategy_list_overloads, Engine::get_strategy_list, 0, 4);

	size_t list_binary_contracts() {
		return list_binary_contracts("", "");
	}

	size_t list_binary_contracts(std::string const & exchange) {
		return list_binary_contracts(exchange, "");
	}
	
	size_t list_binary_contracts(std::string const & exchange,
	                             std::string const & product_code) {
	    size_t id = id_now_++;
	    String s_exchange(exchange);
	    String s_product_code(product_code);
		CR(eng_->listBinaryContracts(s_exchange.r_str(),
		                             s_product_code.r_str(),
		                             (void*)id,
		                             &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			list_binary_contracts_overloads,
			Engine::list_binary_contracts, 0, 2);

	size_t list_exchanges() {
		size_t id = id_now_++;
		CR(eng_->listExchanges((void*)id, &r_errno));
		return id;
	}

	size_t list_trade_routes() {
		size_t id = id_now_++;
		CR(eng_->listTradeRoutes((void*)id, &r_errno));
		return id;
	}

	void rebuild_book(std::string const & exchange,
	                  std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->rebuildBook(s_exchange.r_str(),
		                     s_ticker.r_str(),
		                     &r_errno));
	}

	size_t rebuild_dbo_book(std::string const & exchange,
	                        std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		size_t id = id_now_++;
		CR(eng_->rebuildDboBook(s_exchange.r_str(),
		                        s_ticker.r_str(),
		                        false,
		                        0,
		                        (void*)id,
		                        &r_errno));
		return id;
	}

	size_t rebuild_dbo_book(std::string const & exchange,
	                        std::string const & ticker,
	                        double price) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		size_t id = id_now_++;
		CR(eng_->rebuildDboBook(s_exchange.r_str(),
		                        s_ticker.r_str(),
		                        true,
		                        price,
		                        (void*)id,
		                        &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			rebuild_dbo_book_overloads, Engine::rebuild_dbo_book, 2, 3);

	size_t replay_bars(dict const & params) {
		RApi::ReplayBarParams r_params;
		size_t id = add_bar_params_(r_params, params);
		String sTicker{extract<char*>(params["Ticker"])};
		r_params.sTicker = *sTicker.r_str();
		String sExchange{extract<char*>(params["Exchange"])};
		r_params.sExchange = *sExchange.r_str();
		r_params.iEndSsboe = extract<int>(params.get("EndSsboe", 0));
		r_params.iEndUsecs = extract<int>(params.get("EndUsecs", 0));
		r_params.iStartSsboe = extract<int>(params.get("StartSsboe", 0));
		r_params.iStartUsecs = extract<int>(params.get("StartUsecs", 0));
		String sEndDate{extract<char*>(params.get("EndDate", ""))};
		r_params.sEndDate = *sEndDate.r_str();
		cout << r_params.sEndDate.iDataLen << endl;
		String sStartDate{extract<char*>(params.get("StartDate", ""))};
		r_params.sStartDate = *sStartDate.r_str();
		cout << r_params.sTicker.pData << endl;
		cout << r_params.sExchange.pData << endl;
		CR(eng_->replayBars(&r_params, &r_errno));
		return id;
	}

	size_t replay_quotes(std::string const & account_id) {
		size_t id = id_now_++;
		RApi::AccountInfo & acc = g_accounts.get(account_id);
		CR(eng_->replayQuotes(&acc, (void*)id, &r_errno));
		return id;
	}

	void replay_trades(std::string const & exchange,
	                   std::string const & ticker,
	                   int start_ssboe = 0,
	                   int end_ssboe = 0) {
		String s_exchange(exchange);
		String s_ticker(ticker);
		CR(eng_->replayTrades(s_exchange.r_str(),
		                      s_ticker.r_str(),
		                      start_ssboe,
		                      end_ssboe,
		                      &r_errno));
	 }

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			replay_trades_overloads, Engine::replay_trades, 2, 4);

	void resume_input() {
		CR(eng_->resumeInput(&r_errno));
	}


	size_t search_instrument(std::string const & exchange,
	                         list const & terms) {
		size_t id = id_now_++;
		std::vector<RApi::SearchTerm> rterms;
		for (int i = 0; i < py::len(terms); i++) {
			dict d = extract<dict>(terms[i])();
			RApi::SearchTerm t;
			t.bCaseSensitive = extract<bool>(d["CaseSensitive"]);
			String sTerm{extract<char*>(d["Term"])};
			t.sTerm = copy_r_str(*sTerm.r_str());
			t.eField = (RApi::SearchField)extract<int>(d["Field"])();
			t.eOperator = (RApi::SearchOperator)extract<int>(d["Operator"])();
			// t.eField = SEARCHFIELD_TO_STR.right.at(
			// 		extract<std::string>(d["Field"]));
			// t.eOperator = SEARCHOPERATOR_TO_STR.right.at(
			// 		extract<std::string>(d["Operator"])());
			rterms.push_back(t);
		}
		String s_exchange(exchange);
	    CR(eng_->searchInstrument(s_exchange.r_str(),
		                          &rterms[0],
		                          rterms.size(),
		                          (void*)id,
		                          &r_errno));
		// RApi::SearchTerm * tt = new RApi::SearchTerm[1];
		// tt[0].bCaseSensitive = true;
		// tt[0].sTerm = {"ES", 2};
		// tt[0].eField = RApi::SearchField::ProductCode;
		// tt[0].eOperator = RApi::SearchOperator::Equals;
		// sNCharcb r_exchange{"CME", 3};
	    // CR(eng_->searchInstrument(&r_exchange,
		//                           tt,
		//                           1,
		//                           (void*)id,
		//                           &r_errno));
		for (auto & x : rterms) {
			delete x.sTerm.pData;
		}
		return id;
	}

	void subscribe(std::string const & exchange,
	               std::string const & ticker,
	               int flags) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->subscribe(s_exchange.r_str(),
		                   s_ticker.r_str(),
		                   flags,
		                   &r_errno));
	}


	size_t subscribe_bar(dict const & params) {
		RApi::BarParams r_params;
		size_t id = add_bar_params_(r_params, params);
		String sTicker{extract<char*>(params["Ticker"])};
		r_params.sTicker = *sTicker.r_str();
		String sExchange{extract<char*>(params["Exchange"])};
		r_params.sExchange = *sExchange.r_str();
		CR(eng_->subscribeBar(&r_params, &r_errno));
		return id;
	}

	size_t subscribe_by_underlying(std::string const & underlying,
	                               std::string const & exchange,
	                               std::string const & expiration,
	                               int flags) {
		size_t id = id_now_++;
		String s_exchange(exchange);
		String s_underlying(underlying);
		String s_expiration(expiration);
		CR(eng_->subscribeByUnderlying(s_underlying.r_str(),
		                               s_exchange.r_str(),
		                               s_expiration.r_str(),
		                               flags,
		                               (void*)id,
		                               &r_errno));
		return id;
	}

	void subscribe_dbo(std::string const & exchange,
	                   std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->subscribeDbo(s_exchange.r_str(),
		                      s_ticker.r_str(),
		                      false,
		                      0,
		                      nullptr,
		                      &r_errno));
	}

	void subscribe_dbo(std::string const & exchange,
	                     std::string const & ticker,
	                     double price) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->subscribeDbo(s_exchange.r_str(),
		                      s_ticker.r_str(),
		                      true,
		                      price,
		                      nullptr,
		                      &r_errno));
	}

	size_t subscribe_trade_route(std::string const & fcm_id,
			                     std::string const & ib_id) {
		size_t id = id_now_++;
		String s_fcm_id(fcm_id);
		String s_ib_id(ib_id);
		CR(eng_->subscribeTradeRoute(s_fcm_id.r_str(),
		                             s_ib_id.r_str(),
		                             (void*)id,
		                             &r_errno));
		return id;
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			subscribe_dbo_overloads, Engine::subscribe_dbo, 2, 3);

	void suspend_input() {
		CR(eng_->suspendInput(&r_errno));
	}

	void unsubscribe(std::string const & exchange,
	                 std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->unsubscribe(s_exchange.r_str(), s_ticker.r_str(), &r_errno));
	}

	size_t unsubscribe_bar(dict const & params) {
		RApi::BarParams r_params;
		size_t id = add_bar_params_(r_params, params);
		String sTicker{extract<char*>(params["Ticker"])};
		r_params.sTicker = *sTicker.r_str();
		String sExchange{extract<char*>(params["Exchange"])};
		r_params.sExchange = *sExchange.r_str();
		CR(eng_->unsubscribeBar(&r_params, &r_errno));
		return id;
	}

	void unsubscribe_by_underlying(std::string const & underlying,
	                               std::string const & exchange,
	                               std::string const & expiration) {
		String s_exchange(exchange);
		String s_underlying(underlying);
		String s_expiration(expiration);
		CR(eng_->unsubscribeByUnderlying(s_underlying.r_str(),
		                                 s_exchange.r_str(),
		                                 s_expiration.r_str(),
		                                 &r_errno));
	}

	void unsubscribe_dbo(std::string const & exchange,
	                     std::string const & ticker) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->unsubscribeDbo(s_exchange.r_str(),
		                        s_ticker.r_str(),
		                        false,
		                        0,
		                        nullptr,
		                        &r_errno));
	}

	void unsubscribe_dbo(std::string const & exchange,
	                     std::string const & ticker,
	                     double price) {
	    String s_exchange(exchange);
	    String s_ticker(ticker);
		CR(eng_->unsubscribeDbo(s_exchange.r_str(),
		                        s_ticker.r_str(),
		                        true,
		                        price,
		                        nullptr,
		                        &r_errno));
	}

	BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(
			unsubscribe_dbo_overloads, Engine::unsubscribe_dbo, 2, 3);


	void unsubscribe_trade_route(std::string const & fcm_id,
			                     std::string const & ib_id) {
		String s_fcm_id(fcm_id);
		String s_ib_id(ib_id);
		CR(eng_->unsubscribeTradeRoute(s_fcm_id.r_str(),
		                               s_ib_id.r_str(),
		                               &r_errno));
	}

	void testfun() {
	}


private:


	template <typename Params>
	size_t add_bar_params_(Params & r_params, dict const & params) {
		size_t id = id_now_++;
		r_params.pContext = (void*)id;
		r_params.iType = extract<int>(params["Type"]);
		r_params.bCustomSession =
				extract<bool>(params.get("CustomSession", false));
		r_params.dSpecifiedRange =
				extract<double>(params.get("SpecifiedRange", 0.0));
		r_params.iCloseSsm =
				extract<int>(params.get("CloseSsm", 0));
		r_params.iOpenSsm =
				extract<int>(params.get("OpenSsm", 0));
		r_params.iSpecifiedMinutes =
				extract<int>(params.get("SpecifiedMinutes", 0));
		r_params.iSpecifiedSeconds =
				extract<int>(params.get("SpecifiedSeconds", 0));
		r_params.iSpecifiedTicks =
				extract<int>(params.get("SpecifiedTicks", 0));
		r_params.iSpecifiedVolume =
				extract<int>(params.get("SpecifiedVolume", 0));
		return id;
	}


	std::unique_ptr<MyAdmCallbacks> adm_callbacks_;
	std::unique_ptr<MyCallbacks> callbacks_;
	std::unique_ptr<RApi::REngine> eng_;
	std::string pub_addr_;
	void * zctx_;
	size_t id_now_ = 0;

};


/*****************************************************************************/


BOOST_PYTHON_MODULE(rapiplus) {

 
 	py_OmneException = create_exception_class("OmneException");
 	py_ZMQException = create_exception_class("ZMQException");

	py::register_exception_translator<OmneException>(translate_err_omne);
	py::register_exception_translator<ZMQException>(translate_err_zmq);


	class_<Engine, boost::noncopyable>("Engine", init<std::string>())
		.def("start", &Engine::start)
		.def("start_async", &Engine::start_async)
		.def("get_accounts", &Engine::get_accounts)
		.def("get_equity_option_strategy_list",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &,
		                        std::string const &,
		                        std::string const &)>(&Engine::get_equity_option_strategy_list),
		     Engine::get_equity_option_strategy_list_overloads())
		.def("get_instrument_by_underlying",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &,
		                        std::string const &)>(&Engine::get_instrument_by_underlying),
		     Engine::get_instrument_by_underlying_overloads())
		.def("get_option_list",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &,
		                        std::string const &)>(&Engine::get_option_list),
		     Engine::get_option_list_overloads())
		.def("get_price_incr_info", &Engine::get_price_incr_info)
		.def("get_ref_data", &Engine::get_ref_data)
		.def("get_strategy_info", &Engine::get_strategy_info)
		.def("get_strategy_list",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &,
		                        std::string const &,
		                        std::string const &)>(&Engine::get_strategy_list),
		     Engine::get_strategy_list_overloads())
		.def("list_binary_contracts",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &)>(&Engine::list_binary_contracts),
		     Engine::list_binary_contracts_overloads())
		.def("list_exchanges", &Engine::list_exchanges)
		.def("list_trade_routes", &Engine::list_trade_routes)
		.def("rebuild_book", &Engine::rebuild_book)
		.def("rebuild_dbo_book",
		     static_cast<size_t(Engine::*)
		                       (std::string const &,
		                        std::string const &,
		                        double)>(&Engine::rebuild_dbo_book),
		     Engine::rebuild_dbo_book_overloads())
		.def("replay_bars", &Engine::replay_bars)
		.def("replay_quotes", &Engine::replay_quotes)
		.def("replay_trades",
		     static_cast<void(Engine::*)
		                     (std::string const &,
		                      std::string const &,
		                      int,
		                      int)>(&Engine::replay_trades),
		     Engine::replay_trades_overloads())
		.def("resume_input", &Engine::resume_input)
		.def("search_instrument", &Engine::search_instrument)
		.def("subscribe", &Engine::subscribe)
		.def("subscribe_bar", &Engine::subscribe_bar)
		.def("subscribe_by_underlying", &Engine::subscribe_by_underlying)
		.def("subscribe_dbo",
		     static_cast<void(Engine::*)
		                     (std::string const &,
		                      std::string const &,
		                      double)>(&Engine::subscribe_dbo),
		     Engine::subscribe_dbo_overloads())
		.def("subscribe_trade_route", &Engine::subscribe_trade_route)
		.def("suspend_input", &Engine::suspend_input)
		.def("unsubscribe", &Engine::unsubscribe)
		.def("unsubscribe_bar", &Engine::unsubscribe_bar)
		.def("unsubscribe_by_underlying", &Engine::unsubscribe_by_underlying)
		.def("unsubscribe_dbo",
		     static_cast<void(Engine::*)
		                     (std::string const &,
		                      std::string const &,
		                      double)>(&Engine::unsubscribe_dbo),
		     Engine::unsubscribe_dbo_overloads())
		.def("unsubscribe_trade_route", &Engine::unsubscribe_trade_route)
		.def("testfun", &Engine::testfun)
	;

}
