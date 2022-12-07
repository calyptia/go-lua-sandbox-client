package luasandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/calyptia/api/types"
	"github.com/hashicorp/go-multierror"
)

type Client struct {
	URL        string
	HTTPClient http.Client
	nextID     int
}

type params struct {
	Records []types.FluentBitLogAttrs `json:"events"`
	Code    string                    `json:"filter"`
}

type request struct {
	JSONRPCVersion string `json:"jsonrpc"`
	ID             int    `json:"id"`
	Method         string `json:"method"`
	Params         params `json:"params"`
}

type rawLogResult struct {
	Result   any    `json:"result,omitempty"`
	ErrorMsg string `json:"error,omitempty"`
}

type response struct {
	JSONRPCVersion string         `json:"jsonrpc"`
	ID             int            `json:"id"`
	Result         []rawLogResult `json:"result,omitempty"`
	Error          *rpcError      `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func New(url string) *Client {
	rv := &Client{
		URL:    url,
		nextID: 1,
		HTTPClient: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:       10,
				IdleConnTimeout:    30 * time.Second,
				DisableCompression: true,
			},
		},
	}

	return rv
}

func (c *Client) Run(ctx context.Context, records []types.FluentBitLog, code string) ([]types.FluentBitLog, error) {
	id := c.nextID
	c.nextID += 1

	eventAttrs := []types.FluentBitLogAttrs{}
	for _, e := range records {
		eventAttrs = append(eventAttrs, e.Attrs)
	}

	reqBody, err := json.Marshal(&request{
		JSONRPCVersion: "2.0",
		ID:             id,
		Method:         "run",
		Params: params{
			Records: eventAttrs,
			Code:    code,
		},
	})
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	httpRes, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpRes.Body.Close()

	resBody, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, err
	}

	if httpRes.StatusCode >= 400 {
		return nil, fmt.Errorf("unexpected status code %d: %s", httpRes.StatusCode, string(resBody))
	}

	var response response
	err = json.Unmarshal(resBody, &response)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal response: %w", err)
	}

	if response.ID != id {
		return nil, fmt.Errorf("mismatch jsonrpc id %q", response.ID)
	}

	if response.JSONRPCVersion != "2.0" {
		return nil, fmt.Errorf("unsupported jsonrpc version %q", response.JSONRPCVersion)
	}

	if response.Error != nil {
		return nil, fmt.Errorf("%d: %s", response.Error.Code, response.Error.Message)
	}

	var rv []types.FluentBitLog

	processResult := func(i int, result rawLogResult) error {
		if result.ErrorMsg != "" {
			return errors.New(result.ErrorMsg)
		}

		// lua code result should contain 3 items: code, timestamp, and record
		resultItems := result.Result.([]any)
		if len(resultItems) != 3 {
			return fmt.Errorf("unexpected returned results length")
		}

		code, ok := resultItems[0].(float64)
		if !ok {
			return fmt.Errorf("unexpected return type for \"code\", got %T", resultItems[0])
		}

		if code == -1 {
			// drop record
			return nil
		}

		if code == 0 {
			// record not modified, use the original
			rv = append(rv, records[i])
			return nil
		}

		var timestamp types.FluentBitTime
		if code == 2 {
			// use the input timestamp
			timestamp = records[i].Timestamp
		} else {
			f, ok := resultItems[1].(float64)
			if !ok {
				return fmt.Errorf("unexpected return type for \"timestamp\", got %T", resultItems[1])
			}
			timestamp = types.FluentBitTime(f)
		}

		add := func(v any) error {
			attrs, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected return type for \"record\", got %T", v)
			}

			rv = append(rv, types.FluentBitLog{
				Timestamp: timestamp,
				Attrs:     attrs,
			})

			return nil
		}

		// case were multiple records were returned as an array.
		if rr, ok := resultItems[2].([]any); ok {
			for _, r := range rr {
				if err := add(r); err != nil {
					return err
				}
			}
		} else {
			if err := add(resultItems[2]); err != nil {
				return err
			}
		}

		return nil
	}

	var errs error
	for i, r := range response.Result {
		if err := processResult(i, r); err != nil {
			errs = multierror.Append(errs, IndexedError{Index: uint(i), Err: err})
		}
	}

	return rv, errs
}
