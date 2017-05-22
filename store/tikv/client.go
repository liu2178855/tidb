// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	goctx "golang.org/x/net/context"
)

const (
	maxConnection     = 200
	dialTimeout       = 5 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	readTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	readTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.

	rpcLabelKV  = "kv"
	rpcLabelCop = "cop"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendReq sends Request.
	SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)
}

// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	// TODO: Refactor ConnPools
	p *Pools
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		p: NewPools(maxConnection, func(addr string) (*Conn, error) {
			return NewConnection(addr, dialTimeout)
		}),
	}
}

// SendReq sends a Request to server and receives Response.
func (c *rpcClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	start := time.Now()
	var label = rpcLabelKV
	if req.Type == tikvrpc.CmdCop {
		label = rpcLabelCop
	}
	defer func() { sendReqHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds()) }()

	conn, err := c.p.GetConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.p.PutConn(conn)
	resp, err := c.callRPC(ctx, conn, req)
	if err != nil {
		conn.Close()
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) callRPC(ctx goctx.Context, conn *Conn, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	resp := &tikvrpc.Response{}
	resp.Type = req.Type
	client := tikvpb.NewTikvClient(conn.ClientConn)
	childCtx, cancel := goctx.WithCancel(ctx)
	defer cancel()
	switch req.Type {
	case tikvrpc.CmdGet:
		r, err := client.KvGet(childCtx, req.Get)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Get = r
		return resp, nil
	case tikvrpc.CmdScan:
		r, err := client.KvScan(childCtx, req.Scan)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Scan = r
		return resp, nil
	case tikvrpc.CmdPrewrite:
		r, err := client.KvPrewrite(childCtx, req.Prewrite)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Prewrite = r
		return resp, nil
	case tikvrpc.CmdCommit:
		r, err := client.KvCommit(childCtx, req.Commit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Commit = r
		return resp, nil
	case tikvrpc.CmdCleanup:
		r, err := client.KvCleanup(childCtx, req.Cleanup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Cleanup = r
		return resp, nil
	case tikvrpc.CmdBatchGet:
		r, err := client.KvBatchGet(childCtx, req.BatchGet)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.BatchGet = r
		return resp, nil
	case tikvrpc.CmdBatchRollback:
		r, err := client.KvBatchRollback(childCtx, req.BatchRollback)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.BatchRollback = r
		return resp, nil
	case tikvrpc.CmdScanLock:
		r, err := client.KvScanLock(childCtx, req.ScanLock)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.ScanLock = r
		return resp, nil
	case tikvrpc.CmdResolveLock:
		r, err := client.KvResolveLock(childCtx, req.ResolveLock)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.ResolveLock = r
		return resp, nil
	case tikvrpc.CmdGC:
		r, err := client.KvGC(childCtx, req.GC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.GC = r
		return resp, nil
	case tikvrpc.CmdRawGet:
		r, err := client.RawGet(childCtx, req.RawGet)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawGet = r
		return resp, nil
	case tikvrpc.CmdRawPut:
		r, err := client.RawPut(childCtx, req.RawPut)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawPut = r
		return resp, nil
	case tikvrpc.CmdRawDelete:
		r, err := client.RawDelete(childCtx, req.RawDelete)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawDelete = r
		return resp, nil
	case tikvrpc.CmdCop:
		r, err := client.Coprocessor(childCtx, req.Cop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Cop = r
		return resp, nil
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
}

func (c *rpcClient) Close() error {
	c.p.Close()
	return nil
}
