package wire_test

import (
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/wire"
)

func cloneStates() map[string]turn.MachineState {
	open, _ := turn.InitializeTurn("t")
	open.PendingInputs = []turn.AgentInput{{ID: "input", Digest: "digest"}}
	model := open
	model.Current = turn.ModelStep{RefValue: turn.StepRef{TurnID: "t", ID: "model"}, Model: "m", Tools: []turn.ToolSpec{{Name: "tool", Ref: "tool"}}}
	emptyModel := model
	ms := model.Current.(turn.ModelStep)
	ms.Tools = []turn.ToolSpec{}
	emptyModel.Current = ms
	calls := []turn.ToolCallState{
		{CallID: "pending", Status: turn.ToolPending},
		{CallID: "executing", Status: turn.ToolExecuting, Arguments: jsonstable.MustParse(`{"a":[1,2]}`)},
		{CallID: "waiting", Status: turn.ToolWaiting, Policy: turn.ApprovalRequired, Waiting: &turn.ResponseRequest{ID: "response"}},
		{CallID: "completed", Status: turn.ToolCompleted, Result: &turn.ToolCallResult{OutputDigest: "output"}},
		{CallID: "failed", Status: turn.ToolFailed, Failure: &turn.ToolCallFailure{Failure: turn.ToolFailure{Class: "failed"}}},
	}
	tool := open
	tool.Current = turn.ToolStep{RefValue: turn.StepRef{TurnID: "t", ID: "tools"}, Source: "model", Calls: calls}
	last := open
	last.LastToolStep = &turn.ToolStep{RefValue: turn.StepRef{TurnID: "t", ID: "last"}, Source: "model", Calls: calls[3:]}
	terminal := last
	terminal.Status = turn.TurnStopped
	terminal.Current = nil
	terminal.Result = &turn.TurnResult{Status: turn.TurnStopped, Failure: &turn.TurnFailure{Class: "stopped"}, UncertainCalls: []turn.CallID{"call"}}
	emptyResult := terminal
	result := *terminal.Result
	result.UncertainCalls = []turn.CallID{}
	emptyResult.Result = &result
	emptyInputs := open
	emptyInputs.PendingInputs = []turn.AgentInput{}
	invalidUTF8 := model
	ms = model.Current.(turn.ModelStep)
	ms.Model = turn.ModelRef("m\xff")
	invalidUTF8.Current = ms
	// Encoding repairs these two different invalid IDs to the same string,
	// so validation must run after that repair, just like Decode.
	repairedIdentity := model
	repairedIdentity.TurnID = turn.TurnID("t\xff")
	repairedModel := ms
	repairedModel.RefValue.TurnID = turn.TurnID("t\xfe")
	repairedIdentity.Current = repairedModel
	// Conversely, two distinct pending IDs become duplicates after repair.
	repairedDuplicates := open
	repairedDuplicates.PendingInputs = []turn.AgentInput{{ID: turn.InputID("i\xff")}, {ID: turn.InputID("i\xfe")}}
	largeInt := model
	largeInt.Usage.TotalTokens = 1<<53 + 1
	maxInt := model
	maxInt.Usage.TotalTokens = int(^uint(0) >> 1)
	invalid := open
	invalid.Current = nil
	invalidLast := last
	ts := *last.LastToolStep
	ts.Calls = []turn.ToolCallState{{CallID: "bad", Status: turn.ToolCompleted}}
	invalidLast.LastToolStep = &ts
	return map[string]turn.MachineState{"open": open, "model": model, "empty tools": emptyModel, "tool": tool, "last": last, "terminal": terminal, "empty uncertain": emptyResult, "empty inputs": emptyInputs, "invalid UTF8": invalidUTF8, "repaired identity": repairedIdentity, "repaired duplicates": repairedDuplicates, "large integer": largeInt, "max integer": maxInt, "invalid current": invalid, "invalid last": invalidLast}
}

// Mutation traverses every reachable mutable field in each variant. Immutable
// JSON values are intentionally opaque; their public byte accessors detach.
func mutateSnapshot(value reflect.Value) {
	if value.Type() == reflect.TypeFor[jsonstable.Value]() {
		return
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			v := reflect.New(value.Elem().Type()).Elem()
			v.Set(value.Elem())
			mutateSnapshot(v)
			value.Set(v)
		}
	case reflect.Pointer:
		if !value.IsNil() {
			mutateSnapshot(value.Elem())
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			mutateSnapshot(value.Field(i))
		}
	case reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			mutateSnapshot(value.Index(i))
		}
		if value.CanSet() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
	case reflect.String:
		if value.CanSet() {
			value.SetString("mutated")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.CanSet() {
			value.SetInt(0)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if value.CanSet() {
			value.SetUint(0)
		}
	}
}

func TestSnapshotCloneMatchesCodecAndDetaches(t *testing.T) {
	codec := wire.Snapshot{}
	for name := range cloneStates() {
		t.Run(name, func(t *testing.T) {
			source := cloneStates()[name]
			raw, encodeErr := codec.Encode(&source)
			expected, decodeErr := codec.Decode(raw)
			clone, err := codec.Clone(&source)
			if (err != nil) != (encodeErr != nil || decodeErr != nil) {
				t.Fatalf("clone err=%v, codec=%v/%v", err, encodeErr, decodeErr)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(clone, expected) {
				t.Fatalf("clone differs from round trip\nclone=%#v\nwire=%#v", clone, expected)
			}
			mutateSnapshot(reflect.ValueOf(&clone))
			after, err := codec.Encode(&source)
			if err != nil || string(after) != string(raw) {
				t.Fatalf("clone mutation changed source: %v", err)
			}
			clone, err = codec.Clone(&source)
			if err != nil {
				t.Fatal(err)
			}
			mutateSnapshot(reflect.ValueOf(&source))
			if !reflect.DeepEqual(clone, expected) {
				t.Fatal("source mutation changed retained clone")
			}
		})
	}
	if _, err := codec.Clone(nil); err == nil {
		t.Fatal("accepted nil state")
	}
}

func BenchmarkSnapshotDetached(b *testing.B) {
	codec := wire.Snapshot{}
	states := cloneStates()
	for _, name := range []string{"model", "tool", "last"} {
		state := states[name]
		b.Run(name+"/codec", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				raw, err := codec.Encode(&state)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = codec.Decode(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(name+"/clone", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := codec.Clone(&state); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
