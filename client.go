package luasandbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/calyptia/api/types"
)

type Client struct {
	url        string
	httpClient http.Client
	nextId     int
}

type params struct {
	Events []types.FluentBitLogAttrs `json:"events"`
	Filter string                    `json:"filter"`
}

type request struct {
	JsonRpcVersion string `json:"jsonrpc"`
	Id             int    `json:"id"`
	Method         string `json:"method"`
	Params         params `json:"params"`
}

type LogResult struct {
	Log   types.FluentBitLog
	Error string
}

type RawLogResult struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

type response struct {
	JsonRpcVersion string         `json:"jsonrpc"`
	Id             int            `json:"id"`
	Result         []RawLogResult `json:"result,omitempty"`
	Error          *rpcError      `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func New(url string) *Client {
	rv := &Client{
		url:    url,
		nextId: 1,
		httpClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
			},
		},
	}

	return rv
}

func (c *Client) Run(events []types.FluentBitLog, filter string) ([]LogResult, error) {
	id := c.nextId
	c.nextId += 1

	eventAttrs := []types.FluentBitLogAttrs{}
	for _, e := range events {
		eventAttrs = append(eventAttrs, e.Attrs)
	}

	reqBody, err := json.Marshal(&request{
		JsonRpcVersion: "2.0",
		Id:             id,
		Method:         "run",
		Params: params{
			Events: eventAttrs,
			Filter: filter,
		},
	})
	if err != nil {
		return nil, err
	}

	httpRes, err := c.httpClient.Post(c.url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer httpRes.Body.Close()

	resBody, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, err
	}

	if httpRes.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP Error (%v): %v", httpRes.Status, string(resBody))
	}

	var response response
	err = json.Unmarshal(resBody, &response)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal json response: %v", string(resBody))
	}

	if response.Id != id {
		return nil, fmt.Errorf("Unexpected response id: %v", string(resBody))
	}

	if response.JsonRpcVersion != "2.0" {
		return nil, fmt.Errorf("Unexpected response RPC version: %v", string(resBody))
	}

	if response.Error != nil {
		return nil, fmt.Errorf("RPC call error: %v (%v)", response.Error.Code, response.Error.Message)
	}

	rv := []LogResult{}
	for i, r := range response.Result {
		if r.Error != "" {
			rv = append(rv, LogResult{Error: r.Error})
			continue
		}

		resultItems := r.Result.([]any)
		if len(resultItems) != 3 {
			return nil, fmt.Errorf("RPC call returned unexpected result (wrong number of items)")
		}

		code := resultItems[0].(float64)
		if code == -1 {
			// drop record
			continue
		}

		if code == 0 {
			// record not modified, use the original
			rv = append(rv, LogResult{Log: events[i]})
			continue
		}

		var timestamp types.FluentBitTime
		if code == 2 {
			// use the input timestamp
			timestamp = events[i].Timestamp
		} else {
			timestamp = types.FluentBitTime(resultItems[1].(float64))
		}

		attrs := resultItems[2]
		if reflect.TypeOf(attrs).Kind() == reflect.Slice {
			// split record
			items := attrs.([]any)
			for _, r := range items {
				item := r.(map[string]any)
				rv = append(rv, LogResult{Log: types.FluentBitLog{
					Timestamp: timestamp,
					Attrs:     item,
				}})
			}
			continue
		}

		rv = append(rv, LogResult{Log: types.FluentBitLog{
			Timestamp: timestamp,
			Attrs:     resultItems[2].(map[string]any),
		}})
	}

	return rv, nil
}
