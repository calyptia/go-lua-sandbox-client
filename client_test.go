package luasandbox_test

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/calyptia/api/types"
	"github.com/calyptia/lua-sandbox-client"
)

func getUrl() string {
	return "http://localhost:5555/jsonrpc"
}

func TestSimpleProcessing(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `
  local i = 10.5
  function cb_filter(tag, ts, record)
    i = i + 1
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return 1, i, record
  end`
	result, err := client.Run(context.Background(), events, filter)

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []types.FluentBitLog{
		{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}},
		{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}},
		{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}},
	}, result)
}

func TestDropRecord(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `function cb_filter(tag, ts, record)
    if record.log == 'two' then return -1 end
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return 1, ts, record
  end`
	result, err := client.Run(context.Background(), events, filter)

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []types.FluentBitLog{
		{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}},
		{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}},
	}, result)
}

func TestIgnoreTimestamp(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `
  local i = 10.5
  function cb_filter(tag, ts, record)
    i = i + 1
    local code = 1
    if record.log == 'one' then code = 2 end
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return code, i, record
  end`
	result, err := client.Run(context.Background(), events, filter)

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []types.FluentBitLog{
		{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}},
		{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}},
		{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}},
	}, result)
}

func TestIgnoreProcessing(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `
  local i = 10.5
  function cb_filter(tag, ts, record)
    i = i + 1
    local code = 1
    if record.log == 'two' or record.log == 'three' then code = 0 end
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return code, i, record
  end`
	result, err := client.Run(context.Background(), events, filter)

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []types.FluentBitLog{
		{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}},
		{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}, result)
}

func TestSplit(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `
  local i = 10.5
  function cb_filter(tag, ts, record)
    i = i + 1
    if record.log == 'one' or record.log == 'three' then
      return 1, i, {
        {log = record.log..'.1'},
        {log = record.log..'.2'},
        {log = record.log..'.3'},
      }
    end
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return 1, i, record
  end`
	result, err := client.Run(context.Background(), events, filter)

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, []types.FluentBitLog{
		{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.1"}},
		{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.2"}},
		{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.3"}},
		{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}},
		{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.1"}},
		{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.2"}},
		{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.3"}},
	}, result)
}

func TestScriptTimeout(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{}
	filter := `
  while true do
  end`
	_, err := client.Run(context.Background(), events, filter)
	assert.EqualError(t, err, "HTTP Error (400 Bad Request): <h1>Script timed out</h1>")
}

func TestScriptError(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{}
	filter := `error('some error')`
	_, err := client.Run(context.Background(), events, filter)
	assert.EqualError(t, err, "RPC call error: 21 (error loading script: [string \"fluentbit.lua\"]:1: some error)")
}

func TestCallbackError(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{
		{Attrs: types.FluentBitLogAttrs{"log": "one"}},
		{Attrs: types.FluentBitLogAttrs{"log": "two"}},
		{Attrs: types.FluentBitLogAttrs{"log": "three"}},
	}
	filter := `
  local i = 10.5
  function cb_filter(tag, ts, record)
    i = i + 1
    local code = 1
    if record.log == 'one' or record.log == 'two' then error('error '..record.log) end
    record.msg = 'record '..record.log
    record.processed = record.log
    record.log = nil
    return code, i, record
  end`
	_, err := client.Run(context.Background(), events, filter)
	assert.EqualError(t, err, "Errors were raised processing one or more records:\n"+
		"error processing event 1: [string \"fluentbit.lua\"]:6: error one\n"+
		"error processing event 2: [string \"fluentbit.lua\"]:6: error two\n")
}
