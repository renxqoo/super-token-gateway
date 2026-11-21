package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
)

var (
	ErrBodyTooLarge      = errors.New("body_too_large")
	ErrInvalidJSON       = errors.New("invalid_json")
	ErrInvalidModel      = errors.New("invalid_model")
	ErrUnmeteredTool     = errors.New("unmetered_tool")
	ErrInvalidUsage      = errors.New("invalid_upstream_usage")
	ErrEventTooLarge     = errors.New("sse_event_too_large")
	ErrTrailingAfterDone = errors.New("data_after_terminal")
)

type ChatRequestFacts struct {
	Model       string
	Stream      bool
	OutputBound int64
	InputBytes  int
	BodyDigest  [32]byte
}

type Usage struct {
	InputTokens       int64
	CachedInputTokens int64
	OutputTokens      int64
	ReasoningTokens   int64
	HasUsage          bool
}

type rawChatRequest struct {
	Model               string          `json:"model"`
	Stream              bool            `json:"stream"`
	MaxTokens           *int64          `json:"max_tokens"`
	MaxCompletionTokens *int64          `json:"max_completion_tokens"`
	WebSearchOptions    json.RawMessage `json:"web_search_options"`
	Tools               []rawTool       `json:"tools"`
}

type rawTool struct {
	Type string `json:"type"`
}

func ParseChatRequest(body []byte, maxBytes int, defaultOutputBound int64) (ChatRequestFacts, error) {
	if len(body) == 0 || len(body) > maxBytes {
		return ChatRequestFacts{}, ErrBodyTooLarge
	}
	var req rawChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ChatRequestFacts{}, ErrInvalidJSON
	}
	if len(req.Model) == 0 || len(req.Model) > 150 {
		return ChatRequestFacts{}, ErrInvalidModel
	}
	if len(req.WebSearchOptions) > 0 && string(req.WebSearchOptions) != "null" {
		return ChatRequestFacts{}, ErrUnmeteredTool
	}
	for _, t := range req.Tools {
		if t.Type != "function" {
			return ChatRequestFacts{}, ErrUnmeteredTool
		}
	}
	bound := defaultOutputBound
	if req.MaxCompletionTokens != nil {
		if *req.MaxCompletionTokens <= 0 {
			return ChatRequestFacts{}, ErrInvalidJSON
		}
		bound = *req.MaxCompletionTokens
	} else if req.MaxTokens != nil {
		if *req.MaxTokens <= 0 {
			return ChatRequestFacts{}, ErrInvalidJSON
		}
		bound = *req.MaxTokens
	}
	return ChatRequestFacts{
		Model:       req.Model,
		Stream:      req.Stream,
		OutputBound: bound,
		InputBytes:  len(body),
		BodyDigest:  sha256.Sum256(body),
	}, nil
}

type rawUsageEnvelope struct {
	Usage *rawUsage `json:"usage"`
}

type rawUsage struct {
	PromptTokens            *int64            `json:"prompt_tokens"`
	CompletionTokens        *int64            `json:"completion_tokens"`
	InputTokens             *int64            `json:"input_tokens"`
	OutputTokens            *int64            `json:"output_tokens"`
	PromptTokensDetails     *rawPromptDetails `json:"prompt_tokens_details"`
	CompletionTokensDetails *rawOutputDetails `json:"completion_tokens_details"`
}

type rawPromptDetails struct {
	CachedTokens *int64 `json:"cached_tokens"`
}

type rawOutputDetails struct {
	ReasoningTokens *int64 `json:"reasoning_tokens"`
}

func ParseUsageFromJSON(body []byte) (Usage, error) {
	if !bytes.Contains(body, []byte(`"usage"`)) {
		return Usage{}, nil
	}
	var env rawUsageEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Usage{}, ErrInvalidUsage
	}
	if env.Usage == nil {
		return Usage{}, nil
	}
	u := env.Usage
	var prompt, output, cached, reasoning int64
	if u.PromptTokens != nil {
		prompt = *u.PromptTokens
	} else if u.InputTokens != nil {
		prompt = *u.InputTokens
	} else {
		return Usage{}, nil
	}
	if u.CompletionTokens != nil {
		output = *u.CompletionTokens
	} else if u.OutputTokens != nil {
		output = *u.OutputTokens
	}
	if u.PromptTokensDetails != nil && u.PromptTokensDetails.CachedTokens != nil {
		cached = *u.PromptTokensDetails.CachedTokens
	}
	if u.CompletionTokensDetails != nil && u.CompletionTokensDetails.ReasoningTokens != nil {
		reasoning = *u.CompletionTokensDetails.ReasoningTokens
	}
	if prompt < 0 || output < 0 || cached < 0 || reasoning < 0 || cached > prompt || reasoning > output {
		return Usage{}, ErrInvalidUsage
	}
	return Usage{
		InputTokens:       prompt - cached,
		CachedInputTokens: cached,
		OutputTokens:      output,
		ReasoningTokens:   reasoning,
		HasUsage:          true,
	}, nil
}

type SSEFramer struct {
	maxEventBytes int
	buf           []byte
	doneSeen      bool
	heldDone      []byte
	usage         Usage
}

func NewSSEFramer(maxEventBytes int) *SSEFramer {
	if maxEventBytes <= 0 {
		maxEventBytes = 1 << 20
	}
	return &SSEFramer{
		maxEventBytes: maxEventBytes,
		buf:           make([]byte, 0, 2048),
	}
}

func (f *SSEFramer) Feed(chunk []byte) (emit []byte, done bool, err error) {
	if len(chunk) == 0 {
		return nil, f.doneSeen, nil
	}
	if f.doneSeen {
		if len(bytes.TrimSpace(chunk)) > 0 {
			return nil, true, ErrTrailingAfterDone
		}
		return nil, true, nil
	}
	if len(f.buf)+len(chunk) > f.maxEventBytes*2 {
		return nil, false, ErrEventTooLarge
	}
	f.buf = append(f.buf, chunk...)
	var out []byte
	for {
		idx, sepLen := findEventBoundary(f.buf)
		if idx < 0 {
			if len(f.buf) > f.maxEventBytes {
				return nil, false, ErrEventTooLarge
			}
			break
		}
		eventEnd := idx + sepLen
		eventRaw := f.buf[:eventEnd]
		if f.doneSeen {
			if len(bytes.TrimSpace(eventRaw)) > 0 {
				return nil, true, ErrTrailingAfterDone
			}
			f.buf = f.buf[eventEnd:]
			continue
		}
		isDone, u, perr := inspectSSEEvent(eventRaw[:idx])
		if perr != nil {
			return nil, false, perr
		}
		if u.HasUsage {
			f.usage = u
		}
		if isDone {
			f.doneSeen = true
			f.heldDone = append(f.heldDone[:0], eventRaw...)
		} else {
			out = append(out, eventRaw...)
		}
		f.buf = f.buf[eventEnd:]
	}
	if len(f.buf) == 0 {
		f.buf = f.buf[:0]
	} else if cap(f.buf) > 8192 && len(f.buf)*4 < cap(f.buf) {
		trimmed := make([]byte, len(f.buf), len(f.buf)*2)
		copy(trimmed, f.buf)
		f.buf = trimmed
	}
	return out, f.doneSeen, nil
}

func (f *SSEFramer) HeldDone() []byte {
	return f.heldDone
}

func (f *SSEFramer) Usage() Usage {
	return f.usage
}

func (f *SSEFramer) Done() bool {
	return f.doneSeen
}

func (f *SSEFramer) HasUnconsumed() bool {
	return len(bytes.TrimSpace(f.buf)) > 0
}

func findEventBoundary(b []byte) (int, int) {
	for i := 0; i < len(b)-1; i++ {
		if b[i] == '\n' && b[i+1] == '\n' {
			return i, 2
		}
		if i+3 < len(b) && b[i] == '\r' && b[i+1] == '\n' && b[i+2] == '\r' && b[i+3] == '\n' {
			return i, 4
		}
	}
	return -1, 0
}

var dataPrefix = []byte("data:")
var doneMarker = []byte("[DONE]")

func inspectSSEEvent(event []byte) (isDone bool, usage Usage, err error) {
	var dataBuf []byte
	lines := 0
	for len(event) > 0 {
		nl := bytes.IndexByte(event, '\n')
		var line []byte
		if nl < 0 {
			line = event
			event = nil
		} else {
			line = event[:nl]
			event = event[nl+1:]
		}
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if !bytes.HasPrefix(line, dataPrefix) {
			continue
		}
		val := line[len(dataPrefix):]
		if len(val) > 0 && val[0] == ' ' {
			val = val[1:]
		}
		if lines == 0 {
			dataBuf = val
		} else {
			if lines == 1 {
				copied := make([]byte, len(dataBuf), len(dataBuf)+len(val)+1)
				copy(copied, dataBuf)
				dataBuf = copied
			}
			dataBuf = append(dataBuf, '\n')
			dataBuf = append(dataBuf, val...)
		}
		lines++
	}
	if lines == 0 {
		return false, Usage{}, nil
	}
	if bytes.Equal(dataBuf, doneMarker) {
		return true, Usage{}, nil
	}
	u, err := ParseUsageFromJSON(dataBuf)
	return false, u, err
}

func CeilingRational(quantity, numerator, denominator int64) (int64, bool) {
	if quantity < 0 || numerator < 0 || denominator < 0 {
		return 0, false
	}
	if numerator == 0 && denominator == 0 {
		return 0, true
	}
	if denominator == 0 {
		return 0, false
	}
	if quantity == 0 || numerator == 0 {
		return 0, true
	}
	if quantity > math.MaxInt64/numerator {
		return 0, false
	}
	prod := quantity * numerator
	if prod > math.MaxInt64-(denominator-1) {
		return 0, false
	}
	return (prod + denominator - 1) / denominator, true
}
