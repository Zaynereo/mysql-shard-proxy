// Package sqlx classifies MySQL statements and extracts shard-key predicates.
//
// The parser covers the statement shapes this proxy can route. Anything else
// returns [UnsupportedError] or a syntax error. It is intentionally not a full
// MySQL parser.
package sqlx

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Kind is a coarse statement class.
type Kind int

const (
	// KindUnknown is a statement the parser did not recognize.
	KindUnknown Kind = iota
	// KindSelect is a SELECT.
	KindSelect
	// KindInsert is an INSERT.
	KindInsert
	// KindUpdate is an UPDATE.
	KindUpdate
	// KindDelete is a DELETE.
	KindDelete
	// KindReplace is a REPLACE.
	KindReplace
	// KindBegin starts a transaction.
	KindBegin
	// KindCommit commits a transaction.
	KindCommit
	// KindRollback rolls back a transaction.
	KindRollback
	// KindSet is a session SET statement.
	KindSet
	// KindUse selects a default database.
	KindUse
	// KindShow is SHOW or similar metadata.
	KindShow
	// KindDescribe is DESCRIBE or DESC.
	KindDescribe
	// KindDDL is schema-changing SQL that must run on every shard.
	KindDDL
	// KindDo is DO.
	KindDo
	// KindEmpty is an empty statement.
	KindEmpty
)

// String returns a stable metrics label.
func (k Kind) String() string {
	switch k {
	case KindSelect:
		return "select"
	case KindInsert:
		return "insert"
	case KindUpdate:
		return "update"
	case KindDelete:
		return "delete"
	case KindReplace:
		return "replace"
	case KindBegin, KindCommit, KindRollback:
		return "txn"
	case KindSet:
		return "set"
	case KindUse:
		return "use"
	case KindShow, KindDescribe:
		return "show"
	case KindDDL:
		return "ddl"
	case KindDo:
		return "do"
	case KindEmpty:
		return "empty"
	default:
		return "other"
	}
}

// WritesData reports whether the statement modifies table data or schema.
func (k Kind) WritesData() bool {
	switch k {
	case KindInsert, KindUpdate, KindDelete, KindReplace, KindDDL:
		return true
	default:
		return false
	}
}

// UnsupportedError is returned for recognized SQL this proxy will not route.
type UnsupportedError struct {
	Reason string
}

func (e *UnsupportedError) Error() string {
	return e.Reason
}

func unsupported(reason string) error {
	return &UnsupportedError{Reason: reason}
}

// Flags are properties that make a multi-shard read incorrect to merge.
type Flags struct {
	Distinct  bool
	GroupBy   bool
	OrderBy   bool
	Limit     bool
	Aggregate bool
}

// UnsafeMerge reports whether concatenating per-shard rows would change the result.
func (f Flags) UnsafeMerge() bool {
	return f.Distinct || f.GroupBy || f.OrderBy || f.Limit || f.Aggregate
}

// Analysis is the routable shape of one statement.
type Analysis struct {
	Kind           Kind
	Tables         []string
	Where          Expr
	HasWhere       bool
	InsertKeys     []Value
	MissingShard   bool
	Flags          Flags
	UseDB          string
	SetGlobal      bool
	Autocommit     *bool
	NamesCharset   string
	Locking        bool
	Explain        bool
	UnsupportedMsg string
}

// Match is the set of shard-key values a statement is constrained to.
type Match struct {
	// All is true when the statement is not constrained to particular keys.
	All bool
	// Keys are canonical shard-key values. For inserts, one entry per row.
	Keys []string
	// PerRow is true when Keys come from INSERT/REPLACE rows and must share a shard.
	PerRow bool
}

// Match resolves shard-key values using args for placeholders.
func (a *Analysis) Match(shardKey string, args []any) (Match, error) {
	if a == nil {
		return Match{}, fmt.Errorf("nil analysis")
	}
	shardKey = strings.ToLower(shardKey)
	if a.Kind == KindInsert || a.Kind == KindReplace {
		if a.MissingShard {
			return Match{}, fmt.Errorf("missing shard key column %s", shardKey)
		}
		keys := make([]string, 0, len(a.InsertKeys))
		for _, v := range a.InsertKeys {
			key, err := resolveValue(v, args)
			if err != nil {
				return Match{}, fmt.Errorf("insert shard key: %w", err)
			}
			keys = append(keys, key)
		}
		if len(keys) == 0 {
			return Match{}, fmt.Errorf("missing shard key column %s", shardKey)
		}
		return Match{Keys: keys, PerRow: true}, nil
	}
	if !a.HasWhere || a.Where == nil {
		return Match{All: true}, nil
	}
	set, err := evalExpr(a.Where, shardKey, args)
	if err != nil {
		return Match{}, err
	}
	if set.all {
		return Match{All: true}, nil
	}
	if len(set.keys) == 0 {
		return Match{}, fmt.Errorf("shard key %s predicate matches no rows", shardKey)
	}
	keys := make([]string, 0, len(set.keys))
	for k := range set.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return Match{Keys: keys}, nil
}

// Value is a literal, NULL, placeholder, or an expression we will not hash.
type Value struct {
	Kind    ValueKind
	Literal string
	Index   int
}

// ValueKind classifies a parsed value.
type ValueKind int

const (
	// ValueNone is the zero kind.
	ValueNone ValueKind = iota
	// ValueLiteral is a canonical integer or string.
	ValueLiteral
	// ValuePlaceholder is a prepared-statement marker.
	ValuePlaceholder
	// ValueNull is SQL NULL.
	ValueNull
	// ValueOther is an expression that cannot be used as a shard key.
	ValueOther
)

// Expr is a WHERE predicate relevant to shard routing.
type Expr interface{ exprNode() }

type exprAnd struct{ L, R Expr }
type exprOr struct{ L, R Expr }
type exprNot struct{ X Expr }
type exprCmp struct {
	Col string
	Op  string
	Val Value
}
type exprIn struct {
	Col  string
	Not  bool
	Vals []Value
}
type exprOpaque struct{ Cols []string }
type exprAny struct{}

func (exprAnd) exprNode()    {}
func (exprOr) exprNode()     {}
func (exprNot) exprNode()    {}
func (exprCmp) exprNode()    {}
func (exprIn) exprNode()     {}
func (exprOpaque) exprNode() {}
func (exprAny) exprNode()    {}

type keySet struct {
	all  bool
	keys map[string]struct{}
}

func evalExpr(e Expr, shardKey string, args []any) (keySet, error) {
	switch n := e.(type) {
	case exprAny:
		return keySet{all: true}, nil
	case exprOpaque:
		for _, c := range n.Cols {
			if c == shardKey {
				return keySet{}, fmt.Errorf("cannot route: shard key %s is used in an unsupported predicate", shardKey)
			}
		}
		return keySet{all: true}, nil
	case exprNot:
		inner, err := evalExpr(n.X, shardKey, args)
		if err != nil {
			return keySet{}, err
		}
		if inner.all {
			return keySet{all: true}, nil
		}
		return keySet{}, fmt.Errorf("cannot route: NOT on shard key %s", shardKey)
	case exprAnd:
		left, err := evalExpr(n.L, shardKey, args)
		if err != nil {
			return keySet{}, err
		}
		right, err := evalExpr(n.R, shardKey, args)
		if err != nil {
			return keySet{}, err
		}
		return intersect(left, right), nil
	case exprOr:
		left, err := evalExpr(n.L, shardKey, args)
		if err != nil {
			return keySet{}, err
		}
		right, err := evalExpr(n.R, shardKey, args)
		if err != nil {
			return keySet{}, err
		}
		return union(left, right), nil
	case exprCmp:
		if n.Col != shardKey {
			return keySet{all: true}, nil
		}
		if n.Op != "=" {
			return keySet{}, fmt.Errorf("cannot route: shard key %s compared with %s", shardKey, n.Op)
		}
		key, err := resolveValue(n.Val, args)
		if err != nil {
			return keySet{}, err
		}
		return keySet{keys: map[string]struct{}{key: {}}}, nil
	case exprIn:
		if n.Col != shardKey {
			return keySet{all: true}, nil
		}
		if n.Not {
			return keySet{}, fmt.Errorf("cannot route: NOT IN on shard key %s", shardKey)
		}
		keys := make(map[string]struct{}, len(n.Vals))
		for _, v := range n.Vals {
			key, err := resolveValue(v, args)
			if err != nil {
				return keySet{}, err
			}
			keys[key] = struct{}{}
		}
		if len(keys) == 0 {
			return keySet{}, fmt.Errorf("empty IN list for shard key %s", shardKey)
		}
		return keySet{keys: keys}, nil
	default:
		return keySet{}, fmt.Errorf("cannot route predicate")
	}
}

func intersect(a, b keySet) keySet {
	if a.all {
		return b
	}
	if b.all {
		return a
	}
	out := make(map[string]struct{})
	for k := range a.keys {
		if _, ok := b.keys[k]; ok {
			out[k] = struct{}{}
		}
	}
	return keySet{keys: out}
}

func union(a, b keySet) keySet {
	if a.all || b.all {
		return keySet{all: true}
	}
	out := make(map[string]struct{}, len(a.keys)+len(b.keys))
	for k := range a.keys {
		out[k] = struct{}{}
	}
	for k := range b.keys {
		out[k] = struct{}{}
	}
	return keySet{keys: out}
}

func resolveValue(v Value, args []any) (string, error) {
	switch v.Kind {
	case ValueLiteral:
		return v.Literal, nil
	case ValueNull:
		return "", fmt.Errorf("shard key is NULL")
	case ValueOther:
		return "", fmt.Errorf("shard key must be an integer, string, or placeholder")
	case ValuePlaceholder:
		if v.Index < 0 || v.Index >= len(args) {
			return "", fmt.Errorf("missing placeholder argument %d", v.Index)
		}
		return canonicalArg(args[v.Index])
	default:
		return "", fmt.Errorf("shard key value is missing")
	}
}

func canonicalArg(v any) (string, error) {
	switch n := v.(type) {
	case nil:
		return "", fmt.Errorf("shard key is NULL")
	case string:
		return n, nil
	case []byte:
		return string(n), nil
	case int:
		return strconv.FormatInt(int64(n), 10), nil
	case int8:
		return strconv.FormatInt(int64(n), 10), nil
	case int16:
		return strconv.FormatInt(int64(n), 10), nil
	case int32:
		return strconv.FormatInt(int64(n), 10), nil
	case int64:
		return strconv.FormatInt(n, 10), nil
	case uint:
		return strconv.FormatUint(uint64(n), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(n), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(n), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(n), 10), nil
	case uint64:
		return strconv.FormatUint(n, 10), nil
	case bool:
		if n {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("unsupported shard key argument type %T", v)
	}
}

func canonicalInt(raw string) (string, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(n, 10), nil
}
