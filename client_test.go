package luasandbox_test

import (
	"reflect"
	"testing"

	"github.com/calyptia/api/types"
	"github.com/calyptia/lua-sandbox-client"
)

func shouldError(t *testing.T, expected string, err error) {
	if err == nil {
		t.Errorf("Expected error: %v", expected)
		return
	}

	if expected != err.Error() {
		t.Errorf("Expected error: %v, got: %v", expected, err.Error())
	}
}

func shouldEqual(t *testing.T, expected, actual []luasandbox.LogResult) {
	if len(expected) != len(actual) {
		t.Errorf("Slices have different lengths. Expected: %v, Actual: %v", len(expected), len(actual))
	}

	for i, v := range expected {
		if !reflect.DeepEqual(v, actual[i]) {
			t.Errorf("Log at index %v doesn't match. Expected: %v, Actual: %v", i, v, actual[i])
		}
	}
}

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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Log: types.FluentBitLog{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}}},
		{Log: types.FluentBitLog{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}}},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}}},
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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Log: types.FluentBitLog{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}}},
		{Log: types.FluentBitLog{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}}},
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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Log: types.FluentBitLog{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}}},
		{Log: types.FluentBitLog{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}}},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}}},
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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Log: types.FluentBitLog{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"msg": "record one", "processed": "one"}}},
		{Log: types.FluentBitLog{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"log": "two"}}},
		{Log: types.FluentBitLog{Timestamp: 0, Attrs: types.FluentBitLogAttrs{"log": "three"}}},
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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Log: types.FluentBitLog{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.1"}}},
		{Log: types.FluentBitLog{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.2"}}},
		{Log: types.FluentBitLog{Timestamp: 11.5, Attrs: types.FluentBitLogAttrs{"log": "one.3"}}},
		{Log: types.FluentBitLog{Timestamp: 12.5, Attrs: types.FluentBitLogAttrs{"msg": "record two", "processed": "two"}}},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.1"}}},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.2"}}},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"log": "three.3"}}},
	}, result)
}

func TestScriptTimeout(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{}
	filter := `
  while true do
  end`
	_, err := client.Run(events, filter)
	shouldError(t, "HTTP Error (400 Bad Request): <h1>Script timed out</h1>", err)
}

func TestScriptError(t *testing.T) {
	client := luasandbox.New(getUrl())

	events := []types.FluentBitLog{}
	filter := `error('some error')`
	_, err := client.Run(events, filter)
	shouldError(t, "RPC call error: 21 (error loading script: [string \"fluentbit.lua\"]:1: some error)", err)
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
	result, err := client.Run(events, filter)

	if err != nil {
		t.Error(err)
	}
	shouldEqual(t, []luasandbox.LogResult{
		{Error: "error processing event 1: [string \"fluentbit.lua\"]:6: error one"},
		{Error: "error processing event 2: [string \"fluentbit.lua\"]:6: error two"},
		{Log: types.FluentBitLog{Timestamp: 13.5, Attrs: types.FluentBitLogAttrs{"msg": "record three", "processed": "three"}}},
	}, result)
}
