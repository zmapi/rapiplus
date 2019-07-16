import rapiplus as rapi
import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import tcache
import re
import pickle
import sys
import csv
import struct
import aiohttp
import inspect
import functools
import numpy as np
import pandas as pd
from bidict import bidict
from sortedcontainers import SortedDict
from collections import defaultdict
from constants import *
from copy import deepcopy
from numbers import Number
from zmapi import fix, Publisher
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime, timedelta
from zmapi.exceptions import *
from zmapi.zmq.utils import *
from zmapi.utils import random_str, delayed, get_timestamp
from zmapi.controller import ConnectorCTL
from zmapi.logging import setup_root_logger, disable_logger
from collections import defaultdict
from uuid import uuid4
from base64 import b64encode, b64decode


################################## CONSTANTS ##################################


PUTCALL_TO_PUTORCALL = {
    "Put": 0,
    "Call": 1,
}

INSTYPE_TO_SECTYPE = bidict({
    "Future": "FUT",
    "Future Option": "OOF",
    "Spread": "MLEG",
})

MARKETMODE_TO_STSTATUS = {
    "End of Day": int(fix.SecurityTradingStatus.PostClose),
    "Open": int(fix.SecurityTradingStatus.ReadyToTrade),
}

EVENT_TO_STEVENT = {
    "No Event": "IGNORE",
}

REASON_TO_HALTREASON = {
    "Surveillance Intervention": int(fix.HaltReason.SurveillanceIntervention),
    "Group Schedule": "IGNORE",
}

AGGSIDE_TO_AGGSIDE = {
    "B": int(fix.AggressorSide.Buy),
    "S": int(fix.AggressorSide.Sell),
}

COND_TO_TRADECOND = {

}

PRICETYPE_TO_SETTLPRICETYPE = {
    "final": 1,
    "theoretical": 2,
    "prelim off tick": 101,
    "prelim on tick": 102,
}

DBOSIDE_TO_MDENTRYTYPE = {
    "B": fix.MDEntryType.Bid,
    "S": fix.MDEntryType.Offer,
}

DBOUPDTYPE_TO_MDUPDACTION = {
    "N": "0",
    "C": "1",
    "D": "2",
}

CAPABILITIES = [
    fix.ZMCap.GetInstrumentFields,
    fix.ZMCap.ListDirectoryOOBSnapshot,
    fix.ZMCap.SecurityDefinitionSubscribe,
    fix.ZMCap.SecurityDefinitionOOBSnapshot,
    fix.ZMCap.SecurityListOOBSnapshot,
    fix.ZMCap.SecurityStatusSubscribe,
    fix.ZMCap.MDSubscribe,
    fix.ZMCap.MDMBP,
    fix.ZMCap.MDMBPIncremental,
    fix.ZMCap.MDMBPExplicitDelete,
    fix.ZMCap.MDBBO,
    fix.ZMCap.MDMBO,
    fix.ZMCap.MDMBPPlusMBO,
    fix.ZMCap.MDSaneMBO,
]

CONNECTOR_FEATURES = {
    "MarketDataSnapshotWaitTime": 5.0,
    "SecurityStatusSnapshotWaitTime": 3.0,
    # "SecurityDefinitionSnapshotWaitTime": 3.0,
}

################################### GLOBALS ###################################


RAPI_PUB_CONN_ADDR = "ipc://rapi-pub"
RAPI_PUB_BIND_ADDR = RAPI_PUB_CONN_ADDR

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.eng = None

g.startup_event = asyncio.Event()
g.data_structures_initialized = asyncio.Event()

g.accounts = []
g.instruments = SortedDict()

g.subscriptions = defaultdict(lambda: 0)
g.dbo_subs = set()

g.volume_bought_sold = defaultdict(lambda: {"vb": 0, "vs": 0, "ts": 0})

g.quote_aggregations = {}
g.dbo_aggregations = defaultdict(lambda: [])

g.security_status_subs = set()
g.ins_def_subs = set()

# RApi is sending ref_data updates too often for some reason.
# Should only send message when something is actually changed.
# g.ins_defs = {}

# placeholder for Logger
L = logging.root


################################# EXCEPTIONS ##################################


class RApiException(Exception):

    def __init__(self, code):
        self.err_code = code
        self.err_str = ErrCode.key(code)
        
    def __str__(self):
        return "{} ({})".format(self.err_str, self.err_code)


################################### HELPERS ###################################


def try_remove(container, x):
    try:
        container.remove(x)
    except KeyError:
        pass


def extract_tickex(ins_id):
    spl = ins_id.split("@")
    if len(spl) != 2:
        raise ValueError("invalid ZMInstrumentID")
    return spl[0], spl[1]


def extract_timestamp(data):
    ts = data["Ssboe"] * 1000000000
    ts += data.get("Usecs", 0) * 1000
    return ts


def extract_source_timestamp(data):
    ts = data["SourceSsboe"] * 1000000000
    if "SourceNsecs" in data:
        ts += data["SourceNsecs"]
    else:
        ts += data.get("SourceUsecs", 0) * 1000
    return ts


def check_if_error(data):
    if data["RpCode"] != 0:
        raise RApiException(data["RpCode"])


async def listen_until(topics, term_pred, timeout=-1):
    sock = g.ctx.socket(zmq.SUB)
    sock.connect(RAPI_PUB_CONN_ADDR)
    for topic in topics:
        sock.subscribe(topic)
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)
    tic = time()
    res = []
    while True:
        if timeout >= 0:
            remaining = timeout - ((time() - tic) * 1000)
            if remaining <= 0:
                break
            evs = await poller.poll(remaining)
            if not evs:
                break
        topic, data = await sock.recv_multipart()
        topic = topic[:-1].decode()
        data = pickle.loads(data)
        res.append((topic, data))
        if term_pred(topic, data):
            break
    sock.close()
    return res


async def listen_one(topic, timeout=-1):
    sock = g.ctx.socket(zmq.SUB)
    sock.connect(RAPI_PUB_CONN_ADDR)
    sock.subscribe(topic)
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)
    evs = await poller.poll(timeout * 1000)
    if not evs:
        return
    frames = await sock.recv_multipart()
    data = pickle.loads(frames[-1])
    check_if_error(data)
    return data


async def listen_id(id, topic=None, timeout=-1):
    if not topic:
        topics = [b""]
    else:
        topics = [topic]
    res = await listen_until(topics,
                             lambda t, d: d["Id"] == id,
                             timeout=timeout)
    res = res[-1][1]
    check_if_error(res)
    return res


async def use_cache(key, coro):
    with tcache.open(g.cache_fn, "r", max_timedelta=g.cache_td) as c:
        try:
            return c[key]
        except KeyError:
            pass
    res = await coro()
    with tcache.open(g.cache_fn, "w") as c:
        c[key] = res
    return res


def cached(fn, key=None):
    async def wrapped(*args):
        if key is None:
            k = repr((fn.__name__,) + args)
        else:
            k = repr((key,) + args)
        with tcache.open(g.cache_fn, "r", max_timedelta=g.cache_td) as c:
            try:
                return c[k]
            except KeyError:
                pass
        res = await fn(*args)
        with tcache.open(g.cache_fn, "w") as c:
            c[k] = res
        return res
    return wrapped


@cached
async def get_option_list(*args):
    return await listen_id(g.eng.get_option_list(*args), b"option_list\0")


@cached
async def get_price_incr_info(exchange, ticker):
    g.eng.get_price_incr_info(exchange, ticker)
    def term_pred(t, d):
        return d["Exchange"] == exchange and d["Ticker"] == ticker
    res = await listen_until([b"price_incr_update\0"], term_pred=term_pred)
    L.debug("got price_incr_update")
    res = res[-1][-1]
    check_if_error(res)
    return res["Increments"]


@cached
async def get_strategy_list(*args):
    return await listen_id(g.eng.get_strategy_list(*args), b"strategy_list\0")


async def get_strategy_info(exchange, ticker):
    return await listen_id(g.eng.get_strategy_info(exchange, ticker),
                           b"strategy\0")


@cached
async def list_exchanges():
    res = await listen_id(g.eng.list_exchanges(), b"exchange_list\0")
    return sorted(res["Exchanges"])


@cached
async def search_instrument(exchange, terms):
    rid = g.eng.search_instrument(exchange, terms)
    res = (await listen_id(rid, b"instrument_search\0"))["Results"]
    return res


async def get_ref_data_fast(ins_id):
    ticker, exchange = extract_tickex(ins_id)
    try:
        r = g.ins_df.loc[ins_id].dropna().to_dict()
        r = {k: np.asscalar(v) if isinstance(v, np.generic) else v
                for k, v in r.items()}
        return r
    except KeyError:
        pass
    terms = [{
        "CaseSensitive": True,
        "Term": ticker,
        "Field": SearchField.Ticker,
        "Operator": SearchOperator.Equals,
    }]
    r = await search_instrument(exchange, terms)
    assert len(r) == 1, len(r)
    return r[0]


def subscribe(ins_id, flags):
    if ins_id in g.security_status_subs:
        flags |= SubscriptionFlag.MarketMode
    if ins_id in g.ins_def_subs:
        flags |= SubscriptionFlag.RefData
    ticker, exchange = extract_tickex(ins_id)
    if flags:
        # flags |= SubscriptionFlag.MarketMode
        old_flags = g.subscriptions.get(ins_id, 0)
        if old_flags == flags:
            return False
        if old_flags != 0:
            g.eng.unsubscribe(exchange, ticker)  # have to unsubscribe first
        # if flags & SubscriptionFlag.Quotes and \
        #         not (old_flags & SubscriptionFlag.Quotes):
        #     g.eng.rebuild_book(exchange, ticker)
        g.eng.subscribe(exchange, ticker, flags)
        g.subscriptions[ins_id] = flags
        if old_flags == 0:
            L.info(f"added subscription: {ins_id}")
        else:
            L.info(f"modified subscription: {ins_id}")
    else:
        if g.subscriptions.get(ins_id, 0) == flags:
            return False
        g.eng.unsubscribe(exchange, ticker)
        g.subscriptions.pop(ins_id)
        L.info(f"removed subscription: {ins_id}")
    return True


def add_to_subscription(ins_id, flags):
    return subscribe(ins_id, g.subscriptions.get(ins_id, 0) | flags)


def remove_from_subscription(ins_id, flags):
    return subscribe(ins_id, g.subscriptions.get(ins_id, 0) & ~flags)


def subscribe_dbo(ins_id):
    ticker, exchange = extract_tickex(ins_id)
    if ins_id in g.dbo_subs:
        return False
    g.eng.rebuild_dbo_book(exchange, ticker)
    g.eng.subscribe_dbo(exchange, ticker)
    g.dbo_subs.add(ins_id)
    L.info(f"subscribed dbo: {ins_id}")
    return True


def unsubscribe_dbo(ins_id):
    ticker, exchange = extract_tickex(ins_id)
    if ins_id not in g.dbo_subs:
        return False
    g.eng.unsubscribe_dbo(exchange, ticker)
    g.dbo_subs.remove(ins_id)
    L.info(f"unsubscribed dbo: {ins_id}")
    return True


async def get_ins_details(ins_id, get_all=False):

    d = {}
    ticker, exchange = extract_tickex(ins_id)

    if get_all:
        d["NoMarketSegments"] = [{
            "ZMMarketID": ins_id,
            "NoTradingSessionRules": [{"ZMTradingSessionID": ins_id}],
        }]
        r = await get_price_incr_info(exchange, ticker)
        if len(r) > 1:
            d["NoMarketSegments"][0]["NoTickRules"] = group = []
            min_incr = 1e100
            # Assuming ascending price order in price levels.
            # Assuming that FirstPrice < LastPrice.
            for row in r:
                pprint(row)
                rule = {}
                if row["FirstOperator"] != Operator.None_:
                    assert row["FirstOperator"] in \
                            (Operator.GreaterThan,
                             Operator.GreaterThanOrEqualTo)
                    rule["StartTickPriceRange"] = row["FirstPrice"]
                if row["LastOperator"] != Operator.None_:
                    assert row["LastOperator"] in \
                            (Operator.LessThan,
                             Operator.LessThanOrEqualTo)
                    rule["EndTickPriceRange"] = row["LastPrice"]
                if "StartTickPriceRange" in rule \
                        and "EndTickPriceRange" in rule:
                    assert rule["EndTickPriceRange"] \
                            > rule["StartTickPriceRange"]
                rule["TickIncrement"] = row["PriceIncr"]
                min_incr = min(min_incr, rule["TickIncrement"])
                group.append(rule)
            d["MinPriceIncrement"] = min_incr
        else:
            d["MinPriceIncrement"] = r[0]["PriceIncr"]

    r = await get_ref_data_fast(ins_id)

    if "Currency" in r:
        d["Currency"] = r["Currency"]
    if "Description" in r:
        d["SecurityDesc"] = r["Description"]
    if "Expiration" in r:
        d["MaturityMonthYear"] = r["Expiration"]
    if "ExpirationTime" in r:
        # why ExpirationTime is returned as str?
        d["MaturityTime"] = int(r["ExpirationTime"]) * 1000000000
    if "InstrumentType" in r:
        ins_type = r["InstrumentType"]
        # using bidict, cannot have duplicate values
        if ins_type == "Future Strategy":
            ins_type = "Spread"
        if ins_type == "Future Option Strategy":
            ins_type = "Spread"
        d["SecurityType"] = INSTYPE_TO_SECTYPE[ins_type]
    if "PutCallIndicator" in r:
        d["PutOrCall"] = PUTCALL_TO_PUTORCALL[r["PutCallIndicator"]]
    d["RelSymTransactTime"] = extract_timestamp(r)
    d["SecurityExchange"] = r["Exchange"]
    d["Symbol"] = r["Ticker"]
    if "StrikePrice" in r:
        d["StrikePrice"] = r["StrikePrice"]
    d["SecurityGroup"] = r["ProductCode"]
    if "Underlying" in r:
        d["NoUnderlyings"] = [{"UnderlyingSymbol": r["Underlying"]}]
    d["ZMInstrumentID"] = "{Ticker}@{Exchange}".format(**r)
    if "MinPriceIncrement" in d and "SinglePointValue" in r:
        d["MinPriceIncrementAmount"] = \
                r["SinglePointValue"] * d["MinPriceIncrement"]

    if get_all:
        mseg = d["NoMarketSegments"][0]
        if "FloorPrice" in r:
            mseg["LowLimitPrice"] = r["FloorPrice"]
        if "CapPrice" in r:
            mseg["HighLimitPrice"] = r["CapPrice"]
        if "MaxPriceVariation" in r:
            mseg["MaxPriceVariation"] = r["MaxPriceVariation"]
        tsr = mseg["NoTradingSessionRules"][0]
        if "IsTradable" in r:
            tsr["ZMTradable"] = r["IsTradable"]
        if d.get("SecurityType") == "MLEG":
            r2 = await get_strategy_info(exchange, ticker)
            d["SecuritySubType"] = r2["Type"]
            d["NoLegs"] = group = []
            for x in r2["Legs"]:
                d2 = {}
                d2["ZMLegInstrumentID"] = \
                        "{Ticker}@{Exchange}".format(**x)
                d2["LegRatioQty"] = abs(x["Ratio"])
                d2["LegSide"] = "1" if x["Ratio"] > 0 else "2"
                group.append(d2)

    # for discovery
    if "StrikeOperator" in r \
            or "PeriodCode" in r \
            or "TradingTicker" in r \
            or "OpenTime" in r:
        pprint(r)

    return d

    # ignored:
    # - OpenTime
    # - BinaryContractType
    # - PeriodCode (what is this?)
    # - StrikeOperator (what does this mean?)
    # - TradingExchange (necessary?)
    # - TradingTicker (necessary?)


###############################################################################


class MyController(ConnectorCTL):


    def __init__(self, sock_dn, ins_fields):
        super().__init__(sock_dn,
                         "rapiplus",
                         name="MD",
                         caps=CAPABILITIES,
                         feats=CONNECTOR_FEATURES,
                         ins_fields=ins_fields)


    async def MarketDataRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        if "ZMMarketID" in body:
            raise RejectException("ZMMarketID not supported")
        if "ZMTradingSessionID" in body:
            raise RejectException("ZMTradingSessionID not supported")
        srt = body["SubscriptionRequestType"]
        ins_id = body["ZMInstrumentID"]
        try:
            ticker, exchange = extract_tickex(ins_id)
        except ValueError:
            raise RejectException("invalid ZMInstrumentID")
        if srt == fix.SubscriptionRequestType.Snapshot:
            raise RejectException("unsychronized snapshots not supported",
                                  fix.ZMRejectReason.InvalidValue,
                                  "SubscriptionRequestType")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = rbody = {}
        if srt == fix.SubscriptionRequestType.Unsubscribe:
            subscribe(ins_id, 0)
            unsubscribe_dbo(ins_id)
            return res
        mbp_book = body["ZMMBPBook"]
        mbo_book = body["ZMMBOBook"]
        market_depth = body["MarketDepth"]
        ets = body["NoMDEntryTypes"]
        flags = 0
        has_star = "*" in ets
        if has_star or fix.MDEntryType.Trade in ets:
            flags |= SubscriptionFlag.Prints
            flags |= SubscriptionFlag.PrintsCond
        if has_star or fix.MDEntryType.Bid in ets \
                or fix.MDEntryType.Offer in ets:
            flags |= SubscriptionFlag.Best
            if market_depth != 1 or mbp_book:
                flags |= SubscriptionFlag.Quotes
        if has_star or fix.MDEntryType.OpeningPrice in ets:
            flags |= SubscriptionFlag.Open
        if has_star or fix.MDEntryType.ClosingPrice in ets:
            flags |= SubscriptionFlag.Close
        if has_star or fix.MDEntryType.SettlementPrice in ets:
            flags |= SubscriptionFlag.Settlement
        if has_star or fix.MDEntryType.TradingSessionHighPrice in ets or \
                fix.MDEntryType.TradingSessionLowPrice in ets:
            flags |= SubscriptionFlag.HighLow
        if has_star or fix.MDEntryType.TradingSessionTradeVolume in ets:
            flags |= SubscriptionFlag.TradeVolume
        if has_star or fix.MDEntryType.OpenInterest in ets:
            flags |= SubscriptionFlag.OpenInterest
        subscribe(ins_id, flags)
        if mbo_book:
            subscribe_dbo(ins_id)
        else:
            unsubscribe_dbo(ins_id)
        return res


    async def _list_directory_strategy(self, res, parts):
        # sanity check, rapi does weird things when wrong exchange is input
        if parts[0] not in g.instruments \
                or "Strategy" not in g.instruments[parts[0]]:
            raise RejectException("invalid value",
                                  fix.ZMRejectReason.InvalidValue,
                                  "ZMDirPath")
        args = [parts[0]] + parts[2:]
        group = res["Body"]["ZMNoDirEntries"]
        r = await get_strategy_list(*args)
        if len(args) == 1:
            key = "ProductCodes"
        elif len(args) == 2:
            key = "StrategyTypes"
        elif len(args) == 3:
            key = "Expirations"
        elif len(args) == 4:
            key = "Tickers"
        else:
            raise RejectException("invalid value",
                                  fix.ZMRejectReason.InvalidValue,
                                  "ZMDirPath")
        r = sorted(r[key])
        for x in r:
            d = {}
            d["ZMNodeName"] = x
            if key == "Tickers":
                d["ZMInstrumentID"] = "{}@{}".format(x, args[0])
            group.append(d)
        return res


    async def _list_directory_option(self, res, parts):
        # sanity check, rapi does weird things when wrong exchange is input
        if parts[0] not in g.instruments \
                or "Option" not in g.instruments[parts[0]]:
            raise RejectException("invalid value",
                                  fix.ZMRejectReason.InvalidValue,
                                  "ZMDirPath")
        args = [parts[0]] + parts[2:]
        group = res["Body"]["ZMNoDirEntries"]
        r = await get_option_list(*args)
        if len(args) == 1:
            key = "ProductCodes"
        elif len(args) == 2:
            key = "Expirations"
        elif len(args) == 3:
            r = sorted(r["RefData"], key=lambda d: d["Ticker"])
            for data in r:
                d = {}
                d["ZMNodeName"] = data["Ticker"]
                d["ZMInstrumentID"] = "{}@{}".format(data["Ticker"], args[0])
                d["Text"] = data["Description"]
                group.append(d)
            return res
        else:
            raise RejectException("invalid value",
                                  fix.ZMRejectReason.InvalidValue,
                                  "ZMDirPath")
        r = sorted(r[key])
        for x in r:
            d = {}
            d["ZMNodeName"] = x
            group.append(d)
        return res


    async def ZMListDirectory(self, ident, msg_raw, msg):

        path = msg["Body"].get("ZMDirPath", "/")
        parts = [x for x in path.split("/") if x]
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMListDirectoryResponse}
        res["Body"] = body = {}
        body["ZMNoDirEntries"] = group = []

        if path == "/":
            for x in g.instruments:
                d = {}
                d["ZMNodeName"] = x
                group.append(d)
            return res

        exchange = parts[0]
        if len(parts) == 1:
            for x in g.instruments[exchange]:
                d = {}
                d["ZMNodeName"] = x
                group.append(d)
            return res

        general_category = parts[1]
        if general_category == "Strategy":
            return await self._list_directory_strategy(res, parts)
        if general_category == "Option":
            return await self._list_directory_option(res, parts)

        if len(parts) == 2:
            for x in g.instruments[exchange][general_category]:
                d = {}
                d["ZMNodeName"] = x
                group.append(d)
            return res

        ins_type = parts[2]
        if len(parts) == 3:
            for x in g.instruments[exchange]["All"][ins_type]:
                d = {}
                d["ZMNodeName"] = x
                group.append(d)
            return res

        product_code = parts[3]
        if len(parts) == 4:
            for data in g.instruments[exchange]["All"][ins_type][product_code]:
                d = {}
                d["ZMNodeName"] = data["Ticker"]
                if "Description" in data:
                    d["Text"] = data["Description"]
                d["ZMInstrumentID"] = "{Ticker}@{Exchange}".format(**data)
                group.append(d)
            return res

        raise RejectException("invalid value",
                              fix.ZMRejectReason.InvalidValue,
                              "ZMDirPath")


    async def ZMListCommonInstruments(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCommonInstrumentsResponse
        res["Body"] = body = {}
        r = g.ins_df[g.ins_df["InstrumentType"] == "Future"]
        r = {x: x for x in r.index}
        r = json.dumps(r)
        r = b64encode(r.encode()).decode()
        body["ZMCommonInstruments"] = r
        return res


    async def SecurityListRequest(self, ident, msg_raw, msg):

        body = msg["Body"]
        if "MarketID" in body or "MarketSegmentID" in body:
            raise RejectException("MarketID/MarketSegmentID not supported")
        ins_id = body.get("ZMInstrumentID")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMSecurityListRequestResponse
        res["Body"] = {}
        res["Body"]["NoRelatedSym"] = group = []

        if ins_id:
            r = await get_ins_details(ins_id)
            group.append(r)
        else:
            # r = g.ins_df.loc[ins_id].dropna().to_dict()
            sel = np.ones(len(g.ins_df)).astype(bool)
            if "SecurityExchange" in body:
                sel &= g.ins_df["Exchange"] == body["SecurityExchange"]
            if "SecurityGroup" in body:
                sel &= g.ins_df["ProductCode"] == body["SecurityGroup"]
            if "SecurityType" in body:
                ins_type = INSTYPE_TO_SECTYPE.inv[body["SecurityType"]]
                sel &= g.ins_df["InstrumentType"] == ins_type
            if "Symbol" in body:
                sel &= g.ins_df["Ticker"] == body["Symbol"]
            if "SecurityDesc" in body:
                sel &= g.ins_df["Description"] == body["SecurityDesc"]
            if "MaturityMonthYear" in body:
                sel &= g.ins_df["Expiration"] == body["MaturityMonthYear"]
            df = g.ins_df[sel]
            if df.empty:
                raise RejectException("No instruments matching criteria")
            for ins_id in df.index:
                r = await get_ins_details(ins_id)
                group.append(r)

        return res


    async def SecurityDefinitionRequest(self, indent, msg_raw, msg):
        body = msg["Body"]
        ins_id = body["ZMInstrumentID"]
        srt = body["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.Snapshot:
            # raise RejectException("snapshots disabled temporarily")
            res = {}
            res["Header"] = header = {}
            header["MsgType"] = fix.MsgType.ZMSecurityDefinitionRequestResponse
            res["Body"] = d = await get_ins_details(ins_id, get_all=True)
            return res
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMSecurityDefinitionRequestResponse
        res["Body"] = body = {}
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            if ins_id in g.ins_def_subs:
                raise RejectException("already subscribed")
            g.ins_def_subs.add(ins_id)
            try:
                subscribed = add_to_subscription(
                        ins_id, SubscriptionFlag.RefData)
                assert subscribed
            except:
                g.ins_def_subs.remove(ins_id)
                raise
            body["Text"] = "subscribed"
        elif srt == fix.SubscriptionRequestType.Unsubscribe:
            if ins_id not in g.ins_def_subs:
                raise RejectException("already unsubscribed")
            g.ins_def_subs.remove(ins_id)
            try:
                unsubscribed = remove_from_subscription(
                        ins_id, SubscriptionFlag.RefData)
                assert unsubscribed
            except:
                g.ins_def_subs.add(ins_id)
                raise
            body["Text"] = "unsubscribed"
        else:
            raise RejectException("unrecognized SubscriptionRequestType")
        return res


    async def SecurityStatusRequest(self, ident, msg_raw, msg):
        ins_id = msg["Body"]["ZMInstrumentID"]
        try:
            ticker, exchange = extract_tickex(ins_id)
        except ValueError:
            raise RejectException("invalid ZMInstrumentID")
        srt = msg["Body"]["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.Snapshot:
            raise RejectException("unsychronized snapshots not supported",
                                  fix.ZMRejectReason.InvalidValue,
                                  "SubscriptionRequestType")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMSecurityStatusRequestResponse
        res["Body"] = body = {}
        body["ZMInstrumentID"] = ins_id
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            if ins_id in g.security_status_subs:
                raise RejectException("already subscribed")
            g.security_status_subs.add(ins_id)
            try:
                subscribed = add_to_subscription(
                        ins_id, SubscriptionFlag.MarketMode)
                assert subscribed
            except:
                g.security_status_subs.remove(ins_id)
                raise
            body["Text"] = "subscribed"
        elif srt == fix.SubscriptionRequestType.Unsubscribe:
            if ins_id not in g.security_status_subs:
                raise RejectException("already unsubscribed")
            g.security_status_subs.remove(ins_id)
            try:
                unsubscribed = remove_from_subscription(
                        ins_id, SubscriptionFlag.MarketMode)
                assert unsubscribed
            except:
                g.security_status_subs.add(ins_id)
                raise
            body["Text"] = "unsubscribed"
        else:
            raise RejectException("unrecognized SubscriptionRequestType")
        return res


###############################################################################


class Listener:

    
    def __init__(self):
        self._connection_status = defaultdict(lambda: None)
        self._login_successful = defaultdict(lambda: None)


    async def run(self):

        L.debug("listener started ...")
        sock = g.ctx.socket(zmq.SUB)
        sock.connect(RAPI_PUB_CONN_ADDR)
        sock.subscribe(b"")

        while True:

            topic, data = await sock.recv_multipart()
            topic = topic[:-1].decode()
            data = pickle.loads(data)

            fn = type(self).__dict__.get(f"handle_{topic}")
            if fn:
                try:
                    if inspect.iscoroutinefunction(fn):
                        await fn(self, data)
                    else:
                        fn(self, data)
                except Exception:
                    L.exception("error on listener msg handler:")
            else:
                skipped_topics = (
                    "search_instrument",
                    "best_bid_ask_quote",
                    # "best_ask_quote",
                    # "best_bid_quote",
                    "bid_quote",
                    "ask_quote",
                    "limit_order_book",
                    # "price_incr_update",
                    "option_list",
                )
                if topic in skipped_topics:
                    continue
                if "Underlyings" in data:
                    data["Underlyings"] = sorted(data["Underlyings"])
                if data.get("RpCode"):
                    data["ErrString"] = ErrCode.key(data["RpCode"])
                L.info("{}:\n{}".format(topic, pformat(data)))

            

    def handle_alert(self, data):

        if data["AlertType"] in [AlertType.ConnectionOpened,
                                 AlertType.ConnectionClosed,
                                 AlertType.ConnectionBroken]:
            self._connection_status[data["ConnectionId"]] = data["AlertType"]
        elif data["AlertType"] == AlertType.LoginComplete:
            self._login_successful[data["ConnectionId"]] = True
        elif data["AlertType"] == AlertType.LoginFailed:
            self._login_successful[data["ConnectionId"]] = False
        # pprint(self._login_successful)
        if len(self._login_successful) == 4:
            g.startup_event.set()

        if data["AlertType"] == AlertType.ServiceError \
                and data["ConnectionId"] == ConnectionType.MarketData \
                and data["RpCode"] == ErrCode.NoData \
                and re.search(r"subscription.*aborted.*data.*not available",
                              data.get("Message", "")):
            print("JEEEEE")
            ins_id = "{Ticker}@{Exchange}".format(**data)
            g.subscriptions.pop(ins_id, None)
            try_remove(g.dbo_subs, ins_id)
            try_remove(g.ins_def_subs, ins_id)
            try_remove(g.security_status_subs, ins_id)
            # pprint(g.subscriptions)

        s = "[{AlertType}|{ConnectionId}|{RpCode}] ".format(**data)
        if data["RpCode"] != 0:
            s += "[{}] ".format(ErrCode.key(data["RpCode"]))
        s += data["Message"]
        if "Ticker" in data:
            s += " ({Ticker}@{Exchange})".format(**data)
        L.info(s)


    def handle_account_list(self, data):
        check_if_error(data)
        res = {}
        for d in data["Accounts"]:
            res[d["AccountId"]] = d
        g.accounts = res
        L.info("accounts: {}".format(sorted(g.accounts.keys())))


    async def handle_market_mode(self, data):
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["TransactTime"] = extract_timestamp(data)
        body["ZMInstrumentID"] = ins_id
        body["Text"] = "Reason: {}".format(data["Reason"])
        st_status = MARKETMODE_TO_STSTATUS.get(data["MarketMode"])
        inspect_output = False
        if type(st_status) is int:
            body["SecurityTradingStatus"] = st_status
        elif st_status is None:
            inspect_output = True
        st_event = EVENT_TO_STEVENT.get(data["Event"])
        if type(st_event) is int:
            body["SecurityTradingEvent"] = st_event
        elif st_event is None:
            inspect_output = True
        if data["MarketMode"] not in ("Open", "End of Day"):
            halt_reason = REASON_TO_HALTREASON.get(data["Reason"])
            if type(halt_reason) is int:
                body["HaltReason"] = halt_reason
            elif halt_reason is None:
                inspect_output = True
        if inspect_output:
            L.warning("TODO: inspect unexpected data:\n{}"
                      .format(pformat(data)))
        await g.pub.publish(fix.MsgType.SecurityStatus, body)


    def handle_ref_data(self, data):
        ins_id = "{Ticker}@{Exchange}".format(**data)
        async def handle_ref_data2():
            body = await get_ins_details(ins_id, get_all=True)
            await g.pub.publish(fix.MsgType.SecurityDefinition, body)
        create_task(handle_ref_data2())


    async def _handle_trade_print_generic(self, data):
        if "MDEntryPx" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.New
        d["MDEntryType"] = fix.MDEntryType.Trade
        d["MDEntryPx"] = data["Price"]
        d["MDEntrySize"] = data["Size"]
        d["TransactTime"] = extract_source_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        if "AggressorSide" in data:
            d["AggressorSide"] = AGGSIDE_TO_AGGSIDE[data["AggressorSide"]]
        if "Condition" in data:
            tradecond = COND_TO_TRADECOND.get(data["Condition"])
            if tradecond is None:
                L.warning("Unexpected trade condition:\n{}"
                          .format(pformat(data)))
            elif type(tradecond) is str:
                d["ZMNoTradeConditions"] = [tradecond]
        if "ExchOrdId" in data:
            d["OrderID"] = data["ExchOrdId"]
        if "AggressorExchOrdId" in data:
            d["ZMAggressorOrderID"] = data["AggressorExchOrdId"]
        if "VolumeBought" in data:
            vbs = g.volume_bought_sold[ins_id]
            vbs["vb"] = data["VolumeBought"]
            vbs["ts"] = d["TransactTime"]
        if "VolumeSold" in data:
            vbs = g.volume_bought_sold[ins_id]
            vbs["vs"] = data["VolumeSold"]
            vbs["ts"] = d["TransactTime"]
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_trade_print(self, data):
        await self._handle_trade_print_generic(data)


    async def handle_trade_condition(self, data):
        await self._handle_trade_print_generic(data)


    async def handle_trade_volume(self, data):
        if "TotalVolume" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.TradingSessionTradeVolume
        d["MDEntrySize"] = data["TotalVolume"]
        d["TransactTime"] = extract_source_timestamp(data)
        vbs = g.volume_bought_sold[ins_id]
        if vbs["ts"] == d["TransactTime"]:
            d["BuyVolume"] = vbs["vb"]
            d["SellVolume"] = vbs["vs"]
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_close_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.ClosingPrice
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_high_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.TradingSessionHighPrice
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_low_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.TradingSessionLowPrice
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_open_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.OpeningPrice
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_settlement_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.SettlementPrice
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        if "PriceType" in data:
            spt = PRICETYPE_TO_SETTLPRICETYPE.get(data["PriceType"])
            if spt is None:
                L.warning("TODO: inspect unexpected settlement_price data:\n{}"
                        .format(pformat(data)))
            else:
                d["SettlPriceType"] = spt
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_open_interest(self, data):
        if "Quantity" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.OpenInterest
        d["MDEntrySize"] = data["Quantity"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_high_bid_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.TradingSessionHighBid
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_low_ask_price(self, data):
        if "Price" not in data:
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        d["MDEntryType"] = fix.MDEntryType.TradingSessionLowOffer
        d["MDEntryPx"] = data["Price"]
        d["TransactTime"] = extract_timestamp(data)
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_limit_order_book(self, data):
        ins_id = "{Ticker}@{Exchange}".format(**data)
        L.debug(f"limit_order_book received for {ins_id}")
        body = {}
        body["NoMDEntries"] = group = []
        ts = extract_timestamp(data)
        # tid = g.ctl.insid_to_tid[ins_id]
        body["MDBookType"] = fix.MDBookType.PriceDepth
        d = {}
        d["MDEntryType"] = fix.MDEntryType.EmptyBook
        d["ZMInstrumentID"] = ins_id
        d["MDBookType"] = fix.MDBookType.PriceDepth
        group.append(d)
        for x in data["Bids"]:
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Bid
            d["MDEntrySize"] = x["Size"]
            d["MDEntryPx"] = x["Price"]
            d["TransactTime"] = ts
            if "NumOrders" in x:
                d["NumberOfOrders"] = x["NumOrders"]
            group.append(d)
            if x.get("ImpliedSize"):
                d = {}
                d["MDEntryType"] = fix.MDEntryType.SimulatedSellPrice
                d["MDEntrySize"] = x["ImpliedSize"]
                d["MDEntryPx"] = x["Price"]
                d["MDUpdateAction"] = fix.MDUpdateAction.New
                d["TransactTime"] = ts
                group.append(d)
        for x in data["Asks"]:
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Offer
            d["MDEntrySize"] = x["Size"]
            d["MDEntryPx"] = x["Price"]
            d["TransactTime"] = ts
            if "NumOrders" in x:
                d["NumberOfOrders"] = x["NumOrders"]
            group.append(d)
            if x.get("ImpliedSize"):
                d = {}
                d["MDEntryType"] = fix.MDEntryType.SimulatedBuyPrice
                d["MDEntrySize"] = x["ImpliedSize"]
                d["MDEntryPx"] = x["Price"]
                d["MDUpdateAction"] = fix.MDUpdateAction.New
                d["TransactTime"] = ts
                group.append(d)
        pprint(body)
        # if not group:
        #     L.debug(f"empty order book: {ins_id}")
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)
                


    async def _handle_bidask_quote_generic(self, data, is_bid, is_best):
        if data["UpdateType"] == UpdateType.Undefined:
            # some weird updates where leaning price is emitted...
            return
        if "Price" not in data:
            L.warning("unexpected bid/ask quote:\n{}".format(pformat(data)))
            return
        ins_id = "{Ticker}@{Exchange}".format(**data)
        ut = data["UpdateType"]
        ts = extract_timestamp(data)
        if not is_best:
            L.debug("{}: {} {} @ {} (ut: {})"
                    .format(ins_id, "BID" if is_bid else "ASK",
                            data["Size"], data["Price"], ut))
        if ut in (UpdateType.Begin, UpdateType.Middle, UpdateType.End):
            # print("bid:", data["UpdateType"])
            if ut == UpdateType.Begin:
                if ins_id in g.quote_aggregations:
                    L.error(f"Previous quote aggregation not finished on {ins_id}")
                g.quote_aggregations[ins_id] = (0, [])
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
            d["MDEntryType"] = \
                    fix.MDEntryType.Bid if is_bid else fix.MDEntryType.Offer
            d["MDEntryPx"] = data["Price"]
            d["MDEntrySize"] = data["Size"]
            if "NumOrders" in data:
                d["NumberOfOrders"] = data["NumOrders"]
            qa = g.quote_aggregations[ins_id]
            ts_old, quotes = qa
            if quotes:
                if ts != ts_old:
                    L.warning("Timestamp changed during quote aggregation:\n"
                              "Old quotes: {}\ndata: {}"
                              .format(pformat(quotes), pformat(data)))
                    ts = ts_old
            else:
                d["ZMInstrumentID"] = ins_id
            d["TransactTime"] = ts
            quotes.append(d)
            if data.get("ImpliedSize"):
                d = {}
                d["MDEntryType"] = \
                        fix.MDEntryType.SimulatedSellPrice if is_bid \
                        else fix.MDEntryType.SimulatedBuyPrice
                d["MDEntrySize"] = data["ImpliedSize"]
                d["MDEntryPx"] = data["Price"]
                d["TransactTime"] = ts
                d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
                quotes.append(d)
            g.quote_aggregations[ins_id] = (ts, quotes)
        if not is_best and ut not in (UpdateType.Solo,
                                      UpdateType.End,
                                      UpdateType.Aggregated):
            return
        body = {}
        if ut == UpdateType.End:
            body["NoMDEntries"] = g.quote_aggregations.pop(ins_id)[1]
            # if len(body["NoMDEntries"]) > 10:
            #     yyy = {x["MDEntryPx"] for x in body["NoMDEntries"]}
            #     if len(yyy) > 1:
            #         import ipdb; ipdb.set_trace()
        else:
            body["NoMDEntries"] = group = []
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
            d["MDEntryType"] = \
                    fix.MDEntryType.Bid if is_bid else fix.MDEntryType.Offer
            d["MDEntryPx"] = data["Price"]
            d["MDEntrySize"] = data["Size"]
            if "NumOrders" in data:
                d["NumberOfOrders"] = data["NumOrders"]
            d["ZMInstrumentID"] = ins_id
            d["TransactTime"] = extract_timestamp(data)
            if is_best:
                d["MDPriceLevel"] = 1
            group.append(d)
            if data.get("ImpliedSize"):
                d = {}
                d["MDEntryType"] = \
                        fix.MDEntryType.SimulatedSellPrice if is_bid \
                        else fix.MDEntryType.SimulatedBuyPrice
                d["MDEntrySize"] = data["ImpliedSize"]
                d["MDEntryPx"] = data["Price"]
                d["TransactTime"] = ts
                d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
                if is_best:
                    d["MDPriceLevel"] = 1
                group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_bid_quote(self, data):
        await self._handle_bidask_quote_generic(data, True, False)


    async def handle_best_bid_quote(self, data):
        await self._handle_bidask_quote_generic(data, True, True)


    async def handle_ask_quote(self, data):
        await self._handle_bidask_quote_generic(data, False, False)


    async def handle_best_ask_quote(self, data):
        await self._handle_bidask_quote_generic(data, False, True)


    async def handle_dbo(self, data):
        ins_id = "{Ticker}@{Exchange}".format(**data)
        dut = data.get("DboUpdateType")
        # print("dbo", dut)
        if data["Type"] == DataType.Image:
            # aggregation terminated by dbo_book_rebuild
            key = (ins_id, data["Id"])
            dagg = g.dbo_aggregations[key]
            if not dagg:
                d = {}
                # TODO: add zmapi documentation about this
                d["MDEntryType"] = fix.MDEntryType.EmptyBook
                d["MDBookType"] = fix.MDBookType.OrderDepth
                d["ZMInstrumentID"] = ins_id
                dagg.append(d)
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = DBOSIDE_TO_MDENTRYTYPE[data["Side"]]
            d["OrderID"] = data["ExchOrdId"]
            # d["TransactTime"] = extract_source_timestamp(data)
            d["MDEntryPx"] = data["Price"]
            d["MDEntrySize"] = data["Size"]
            d["MDEntryPositionNo"] = int(data["Priority"])
            dagg.append(d)
            return
        body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["ZMInstrumentID"] = ins_id
        d["MDEntryType"] = DBOSIDE_TO_MDENTRYTYPE[data["Side"]]
        d["OrderID"] = data["ExchOrdId"]
        d["MDEntryPx"] = data["Price"]
        d["MDEntrySize"] = data["Size"]
        d["MDEntryPositionNo"] = int(data["Priority"])
        d["MDUpdateAction"] = DBOUPDTYPE_TO_MDUPDACTION[data["DboUpdateType"]]
        d["TransactTime"] = extract_source_timestamp(data)
        group.append(d)
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)


    async def handle_dbo_book_rebuild(self, data):
        print("dbo_book_rebuild", pformat(data))
        ins_id = "{Ticker}@{Exchange}".format(**data)
        key = (ins_id, data["Id"])
        dagg = g.dbo_aggregations.pop(key)
        if not dagg:
            L.warning(f"Empty dbo aggregation: {key}")
            return
        body = {"NoMDEntries": dagg}
        await g.pub.publish(fix.MsgType.MarketDataIncrementalRefresh, body)
    

###############################################################################


@cached
async def init_instrument_tree():
    L.debug("initializing search_instrument based tree ...")
    r = await list_exchanges()
    L.debug("{} exchanges: {}".format(len(r), r))
    res = SortedDict()
    for exchange in r:
       terms = [
           {
               "CaseSensitive": True,
               "Term": exchange,
               "Field": SearchField.Exchange,
               "Operator": SearchOperator.Equals,
           }
       ]
       print(exchange)
       await asyncio.sleep(60)
       r = await search_instrument(exchange, terms)
       instrument_types = set(x.get("InstrumentType", "") for x in r)
       L.debug("{} instrument types: {}"
               .format(exchange, sorted(instrument_types)))
       res[exchange] = SortedDict()
       res[exchange]["All"] = root = SortedDict()
       for instrument_type in instrument_types:
           product_codes = set(x.get("ProductCode", "") for x in r
                               if x.get("InstrumentType") == instrument_type)
           L.debug("{}/{} has {} product codes"
                   .format(exchange, instrument_type, len(product_codes)))
           root[instrument_type] = SortedDict()
           for product_code in product_codes:
               sel = [x for x in r
                      if x.get("ProductCode") == product_code and \
                         x.get("InstrumentType") == instrument_type]
               sel = sorted(sel, key=lambda d: d["Ticker"])
               root[instrument_type][product_code] = sel
    return res


@cached
async def init_instrument_dataframe():
    L.debug("initializing instrument dataframe ...")
    r = await list_exchanges()
    res = []
    for exchange in r:
        terms = [{
            "CaseSensitive": True,
            "Term": exchange,
            "Field": SearchField.Exchange,
            "Operator": SearchOperator.Equals,
        }]
        r = await search_instrument(exchange, terms)
        res += r
    res = pd.DataFrame(res)
    res["ZMInstrumentID"] = res.apply(
            lambda d: "{Ticker}@{Exchange}".format(**d), axis=1)
    res = res.set_index("ZMInstrumentID")
    res = res.sort_index()
    return res


def gen_instrument_fields():
    res = []
    d = {}
    d["ZMFieldName"] = "SecurityExchange"
    d["ZMOptionsInclude"] = sorted(g.instruments.keys())
    res.append(d)
    d = {}
    d["ZMFieldName"] = "SecurityGroup"
    res.append(d)
    d = {}
    d["ZMFieldName"] = "SecurityType"
    d["ZMOptionsInclude"] = sorted(INSTYPE_TO_SECTYPE.values())
    res.append(d)
    d = {}
    d["ZMFieldName"] = "Symbol"
    res.append(d)
    d = {}
    d["ZMFieldName"] = "MaturityMonthYear"
    res.append(d)
    return res

    
async def init_data_structures():

    L.debug("initializing data structures ...")

    assert g.accounts

    g.instruments = await init_instrument_tree()

    g.ins_df = await init_instrument_dataframe()
    L.debug("{} instruments loaded in the dataframe".format(len(g.ins_df)))
    
    try:
        res = sorted((await get_strategy_list())["Exchanges"])
    except RApiException as err:
        L.warning("failed to get strategy list: {}".format(err))
    else:
        L.debug("strategy_list exchanges: {}".format(res))
        for exchange in res:
            if exchange not in g.instruments:
                g.instruments[exchange] = SortedDict()
            g.instruments[exchange]["Strategy"] = SortedDict()

    try:
        res = sorted((await get_option_list())["Exchanges"])
    except RApiException as err:
        L.warning("failed to get option list: {}".format(err))
    else: 
        L.debug("option_list exchanges: {}".format(res))
        for exchange in res:
            if exchange not in g.instruments:
                g.instruments[exchange] = SortedDict()
            g.instruments[exchange]["Option"] = SortedDict()

    # TODO: list_binary_contracts
    # TODO: get_equity_option_strategy_list

    g.data_structures_initialized.set()


async def testing():
    L.debug("testing running ...")

    # g.eng.list_exchanges()

    # g.eng.search_instrument("CME", terms)


    # g.eng.rebuild_dbo_book("CME", "ESZ9", 100)

    # g.ctl.insid_to_tid["ESH9@CME"] = -100
    # g.eng.subscribe("CME", "ESH9", SubscriptionFlag.All)
    #g.eng.subscribe_dbo("CME", "ESH9")
    # await asyncio.sleep(5)
    # g.eng.unsubscribe_dbo("CME", "ESH9")
    # g.eng.rebuild_dbo_book("CME", "ESH9")
    # g.eng.subscribe("CME", "ESH0", SubscriptionFlag.All)
    # g.eng.subscribe_dbo("CME", "ESH0", 123)
    # g.eng.subscribe("CME", "ESM9-ESH0", SubscriptionFlag.All)
    # g.eng.subscribe_dbo("CME", "ESM9-ESH0", 123)

    # g.eng.get_strategy_list("CME", "ES", "Futures Calendar", "201906")
    # g.eng.get_strategy_list()

    # g.eng.get_price_incr_info("CME", "ESZ9")
    # g.eng.get_strategy_info("CME", "ESM9-ESH0")

    # g.eng.get_instrument_by_underlying("ES", "CME")
    # g.eng.get_instrument_by_underlying("KC  FMN0020!", "NYBOT", "20200612")

    # g.eng.get_equity_option_strategy_list()

    # g.eng.subscribe("LIFFE", "L   FMH0019_OMPA0000981253032019", SubscriptionFlag.All)
    # g.eng.get_option_list("LIFFE", "200", "201903")
    # g.eng.get_option_list("CME")
    
    # g.eng.get_ref_data("CME", "ESZ9")
   
    # g.eng.list_trade_routes()

    # g.eng.subscribe_trade_route("Rithmic-FCM", "Prospects")
    
    # params = {
    #     "Type": BarType.Minute,
    #     "SpecifiedMinutes": 1,
    #     "Ticker": "ESH9",
    #     "Exchange": "CME"
    # }
    # g.eng.subscribe_bar(params)
    # g.eng.replay_bars(params)
    #
    #g.eng.subscribe_by_underlying("ES", "CME", "201903", SubscriptionFlag.All)
    #g.eng.replay_quotes("SJ8524")
    # g.eng.replay_trades("CME", "ESH9")

###############################################################################


def parse_args():
    parser = argparse.ArgumentParser(description="r|api+ md/ac connector")
    parser.add_argument("md_ctl_addr",
                        help="address to bind to for md ctl socket")
    parser.add_argument("md_pub_addr",
                        help="address to bind to for md pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--cache-fn",
                        default="general.cache",
                        help="cache filename")
    parser.add_argument("--cache-dd", type=float, default=1,
                        help="cache refresh frequency in days")
    args = parser.parse_args()
    g.cache_td = timedelta(days=args.cache_dd)
    g.cache_fn = args.cache_fn
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_root_logger(args.log_level)
    disable_logger("parso.python.diff")
    disable_logger("parso.cache")


def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.md_ctl_addr)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.md_pub_addr)


async def init_controller():
    L.debug("initializing connector ...")
    ins_fields = gen_instrument_fields()
    g.ctl = MyController(g.sock_ctl, ins_fields)
    await g.ctl.run()


def main():
    args = parse_args()
    setup_logging(args)
    tcache.ensure_exists(g.cache_fn)
    init_zmq_sockets(args)
    g.pub = Publisher(g.sock_pub)
    g.listener = Listener()
    # g.pub = Publisher(g.sock_pub)
    L.debug("starting rapi engine ...")
    g.eng = rapi.Engine(RAPI_PUB_BIND_ADDR)
    g.eng.start_async()
    L.debug("starting event loop ...")
    tasks = [
        g.listener.run(),
        delayed(testing, g.startup_event),
        delayed(init_data_structures, g.startup_event),
        delayed(init_controller, g.data_structures_initialized),
        #delayed(g.ctl.run, g.startup_event),
        #g.pub.run(),
    ]
    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    L.debug("destroying ctx on python side ...")
    g.ctx.destroy()
    L.debug("ctx destroyed on python side")


if __name__ == "__main__":
    main()

