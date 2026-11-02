package sqlx

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkString
	tkNumber
	tkParam
	tkComma
	tkLParen
	tkRParen
	tkDot
	tkOp
	tkSemi
)

type token struct {
	kind  tokenKind
	val   string
	param int
}

type parser struct {
	toks     []token
	i        int
	depth    int
	shardKey string
}

// Parse analyzes a single SQL statement for routing.
// shardKey is the configured column name.
func Parse(sql, shardKey string) (*Analysis, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, shardKey: strings.ToLower(shardKey)}
	p.skipSemis()
	if p.eof() {
		return &Analysis{Kind: KindEmpty}, nil
	}
	a, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	p.skipSemis()
	if !p.eof() {
		return nil, fmt.Errorf("multi-statements are not supported")
	}
	return a, nil
}

func (p *parser) parseStatement() (*Analysis, error) {
	if p.eof() {
		return &Analysis{Kind: KindEmpty}, nil
	}
	kw := ""
	if p.peek().kind == tkIdent {
		kw = p.peek().val
	}
	switch kw {
	case "select":
		return p.parseSelect()
	case "insert":
		return p.parseInsert(KindInsert)
	case "replace":
		return p.parseInsert(KindReplace)
	case "update":
		return p.parseUpdate()
	case "delete":
		return p.parseDelete()
	case "begin":
		p.pop()
		p.eat("work")
		p.skipRest()
		return &Analysis{Kind: KindBegin}, nil
	case "start":
		p.pop()
		if !p.eat("transaction") {
			return nil, fmt.Errorf("expected START TRANSACTION")
		}
		for p.eat("with") || p.eat("consistent") || p.eat("snapshot") || p.eat("read") || p.eat("only") || p.eat("write") {
		}
		p.skipRest()
		return &Analysis{Kind: KindBegin}, nil
	case "commit":
		p.pop()
		p.eat("work")
		p.skipRest()
		return &Analysis{Kind: KindCommit}, nil
	case "rollback":
		p.pop()
		if p.eat("to") || p.is("savepoint") {
			return nil, unsupported("SAVEPOINT is not supported")
		}
		p.eat("work")
		p.skipRest()
		return &Analysis{Kind: KindRollback}, nil
	case "savepoint", "release":
		return nil, unsupported("SAVEPOINT is not supported")
	case "set":
		return p.parseSet()
	case "use":
		return p.parseUse()
	case "show":
		p.skipRest()
		return &Analysis{Kind: KindShow}, nil
	case "describe", "desc":
		p.skipRest()
		return &Analysis{Kind: KindDescribe}, nil
	case "explain":
		return p.parseExplain()
	case "create", "alter", "drop", "truncate", "rename":
		return p.parseDDL()
	case "do":
		p.skipRest()
		return &Analysis{Kind: KindDo}, nil
	case "lock", "unlock", "handler", "call", "with", "table":
		return nil, unsupported(kw + " is not supported")
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

func (p *parser) parseExplain() (*Analysis, error) {
	p.pop()
	if p.eat("analyze") {
		return nil, unsupported("EXPLAIN ANALYZE is not supported")
	}
	if p.eat("format") {
		p.eatOp("=")
		if !p.eof() {
			p.pop()
		}
	}
	for p.is("extended") || p.is("partitions") || p.is("describe") {
		p.pop()
	}
	inner, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	inner.Explain = true
	return inner, nil
}

func (p *parser) parseDDL() (*Analysis, error) {
	for _, t := range p.toks[p.i:] {
		if t.kind == tkIdent && t.val == "temporary" {
			return nil, unsupported("temporary tables are not supported")
		}
		if t.kind == tkSemi {
			break
		}
	}
	p.skipRest()
	return &Analysis{Kind: KindDDL}, nil
}

func (p *parser) parseUse() (*Analysis, error) {
	p.pop()
	if p.eof() {
		return nil, fmt.Errorf("USE requires a database name")
	}
	tok := p.pop()
	if tok.kind != tkIdent && tok.kind != tkString {
		return nil, fmt.Errorf("USE requires a database name")
	}
	return &Analysis{Kind: KindUse, UseDB: tok.val}, nil
}

func (p *parser) parseSet() (*Analysis, error) {
	a := &Analysis{Kind: KindSet}
	p.pop()
	if p.eat("names") || p.eat("charset") {
		if p.eof() || (p.peek().kind != tkIdent && p.peek().kind != tkString) {
			return nil, fmt.Errorf("SET NAMES requires a charset")
		}
		a.NamesCharset = p.pop().val
		if p.eat("collate") && !p.eof() {
			p.pop()
		}
		return a, nil
	}
	saw := false
	for !p.eof() && !p.isSemi() {
		if p.eat("global") {
			a.SetGlobal = true
		} else {
			p.eat("session")
			p.eat("local")
		}
		if p.eof() || p.peek().kind != tkIdent {
			return nil, fmt.Errorf("expected variable name in SET")
		}
		name := variableBase(p.pop().val)
		if p.peek().kind == tkOp && (p.peek().val == "=" || p.peek().val == ":=") {
			p.pop()
		} else {
			return nil, fmt.Errorf("expected = in SET")
		}
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		if name == "autocommit" && !a.SetGlobal {
			on, ok := autocommitOf(val)
			if !ok {
				return nil, fmt.Errorf("SET autocommit value must be 0, 1, ON, or OFF")
			}
			a.Autocommit = &on
		}
		saw = true
		if !p.eatComma() {
			break
		}
	}
	if !saw {
		return nil, fmt.Errorf("empty SET")
	}
	return a, nil
}

func variableBase(name string) string {
	name = strings.ToLower(name)
	name = strings.TrimPrefix(name, "@@")
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	return name
}

func autocommitOf(v Value) (bool, bool) {
	if v.Kind != ValueLiteral {
		return false, false
	}
	switch strings.ToLower(v.Literal) {
	case "1", "on", "true":
		return true, true
	case "0", "off", "false":
		return false, true
	default:
		return false, false
	}
}

func (p *parser) parseSelect() (*Analysis, error) {
	a := &Analysis{Kind: KindSelect}
	p.pop()
	for {
		switch {
		case p.eat("distinct"), p.eat("distinctrow"):
			a.Flags.Distinct = true
		case p.eat("all"), p.eat("high_priority"), p.eat("straight_join"),
			p.eat("sql_small_result"), p.eat("sql_big_result"), p.eat("sql_buffer_result"),
			p.eat("sql_cache"), p.eat("sql_no_cache"), p.eat("sql_calc_found_rows"):
		default:
			goto list
		}
	}
list:
	for !p.eof() && !p.isSemi() && !(p.depth == 0 && p.isClauseEnd()) {
		if p.depth == 0 && p.is("from") {
			break
		}
		tok := p.peek()
		if tok.kind == tkIdent && isAggregate(tok.val) && p.nextKind(1) == tkLParen {
			a.Flags.Aggregate = true
		}
		if tok.kind == tkLParen {
			p.depth++
		} else if tok.kind == tkRParen {
			if p.depth == 0 {
				return nil, fmt.Errorf("unexpected )")
			}
			p.depth--
		}
		p.pop()
	}
	if p.depth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses")
	}
	if p.eat("from") {
		table, err := p.parseTable()
		if err != nil {
			return nil, err
		}
		a.Tables = []string{table}
		if err := p.rejectJoin(); err != nil {
			return nil, err
		}
	}
	if err := p.parseTrailing(a); err != nil {
		return nil, err
	}
	return a, nil
}

func (p *parser) parseUpdate() (*Analysis, error) {
	a := &Analysis{Kind: KindUpdate}
	p.pop()
	for p.eat("low_priority") || p.eat("ignore") {
	}
	table, err := p.parseTable()
	if err != nil {
		return nil, err
	}
	a.Tables = []string{table}
	if err := p.rejectJoin(); err != nil {
		return nil, err
	}
	if !p.eat("set") {
		return nil, fmt.Errorf("expected SET")
	}
	if err := p.skipUntilBoundary("where", "order", "limit"); err != nil {
		return nil, err
	}
	if err := p.parseTrailing(a); err != nil {
		return nil, err
	}
	return a, nil
}

func (p *parser) parseDelete() (*Analysis, error) {
	a := &Analysis{Kind: KindDelete}
	p.pop()
	for p.eat("low_priority") || p.eat("quick") || p.eat("ignore") {
	}
	if !p.eat("from") {
		return nil, unsupported("multi-table DELETE is not supported")
	}
	table, err := p.parseTable()
	if err != nil {
		return nil, err
	}
	a.Tables = []string{table}
	if err := p.rejectJoin(); err != nil {
		return nil, err
	}
	if err := p.parseTrailing(a); err != nil {
		return nil, err
	}
	return a, nil
}

func (p *parser) parseInsert(kind Kind) (*Analysis, error) {
	a := &Analysis{Kind: kind}
	p.pop()
	for p.eat("low_priority") || p.eat("delayed") || p.eat("high_priority") || p.eat("ignore") {
	}
	p.eat("into")
	table, err := p.parseTable()
	if err != nil {
		return nil, err
	}
	a.Tables = []string{table}
	if p.eat("partition") {
		return nil, unsupported("PARTITION is not supported")
	}
	var cols []string
	if p.peek().kind == tkLParen {
		cols, err = p.parseIdentList()
		if err != nil {
			return nil, err
		}
	}
	if p.is("select") || p.is("with") || p.is("table") {
		return nil, unsupported("INSERT ... SELECT is not supported")
	}
	if p.eat("set") {
		for !p.eof() && !p.isSemi() && !p.is("on") {
			if p.peek().kind != tkIdent {
				return nil, fmt.Errorf("expected column in SET")
			}
			col := p.pop().val
			if p.peek().kind != tkOp {
				return nil, fmt.Errorf("expected = in INSERT SET")
			}
			p.pop()
			val, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			if col == p.shardKey {
				a.InsertKeys = append(a.InsertKeys, val)
			}
			if !p.eatComma() {
				break
			}
		}
		if len(a.InsertKeys) == 0 {
			a.MissingShard = true
		}
		p.skipInsertTail()
		return a, nil
	}
	if !p.eat("values") && !p.eat("value") {
		return nil, fmt.Errorf("expected VALUES")
	}
	shardPos := -1
	for i, c := range cols {
		if c == p.shardKey {
			shardPos = i
		}
	}
	if len(cols) == 0 || shardPos < 0 {
		a.MissingShard = true
	}
	for {
		row, err := p.parseTuple()
		if err != nil {
			return nil, err
		}
		if !a.MissingShard {
			if shardPos >= len(row) {
				return nil, fmt.Errorf("VALUES row is shorter than the column list")
			}
			a.InsertKeys = append(a.InsertKeys, row[shardPos])
		}
		if !p.eatComma() {
			break
		}
	}
	p.skipInsertTail()
	return a, nil
}

func (p *parser) parseTrailing(a *Analysis) error {
	for !p.eof() && !p.isSemi() {
		switch {
		case p.eat("where"):
			expr, err := p.parseOr()
			if err != nil {
				return err
			}
			a.Where = expr
			a.HasWhere = true
		case p.eat("group"):
			if !p.eat("by") {
				return fmt.Errorf("expected GROUP BY")
			}
			a.Flags.GroupBy = true
			if err := p.skipUntilBoundary("order", "limit", "having", "for", "lock", "union", "window", "into"); err != nil {
				return err
			}
		case p.eat("order"):
			if !p.eat("by") {
				return fmt.Errorf("expected ORDER BY")
			}
			a.Flags.OrderBy = true
			if err := p.skipUntilBoundary("limit", "for", "lock", "union", "window", "into"); err != nil {
				return err
			}
		case p.eat("limit"):
			a.Flags.Limit = true
			if err := p.skipUntilBoundary("for", "lock", "union", "window", "into", "offset"); err != nil {
				return err
			}
			if p.eat("offset") {
				if err := p.skipUntilBoundary("for", "lock", "union", "window", "into"); err != nil {
					return err
				}
			}
		case p.eat("having"):
			a.Flags.GroupBy = true
			if err := p.skipUntilBoundary("order", "limit", "for", "lock", "union", "window", "into"); err != nil {
				return err
			}
		case p.eat("for"):
			a.Locking = true
			if err := p.skipUntilBoundary("union", "into"); err != nil {
				return err
			}
		case p.eat("lock"):
			a.Locking = true
			if err := p.skipUntilBoundary("union", "into"); err != nil {
				return err
			}
		case p.eat("union"):
			return unsupported("UNION is not supported")
		case p.eat("into"):
			return unsupported("SELECT INTO is not supported")
		case p.eat("window"):
			if err := p.skipUntilBoundary("order", "limit", "for", "union", "into"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected token %q", p.peek().val)
		}
	}
	return nil
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.depthAllows() && p.eat("or") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = exprOr{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.depthAllows() && p.eat("and") {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = exprAnd{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.eat("not") {
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return exprNot{X: x}, nil
	}
	return p.parsePred()
}

func (p *parser) parsePred() (Expr, error) {
	if p.boundary() {
		return nil, fmt.Errorf("expected predicate")
	}
	if p.peek().kind == tkLParen {
		p.pop()
		p.depth++
		if p.is("select") {
			return nil, unsupported("subqueries are not supported")
		}
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, fmt.Errorf("expected )")
		}
		p.pop()
		p.depth--
		return e, nil
	}
	if !p.looksLikeColumn() {
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		op, ok := p.eatCompare()
		if !ok {
			return exprAny{}, nil
		}
		col, ok := p.parseColumn()
		if !ok {
			return exprOpaque{Cols: nil}, nil
		}
		return exprCmp{Col: col, Op: op, Val: val}, nil
	}
	col, ok := p.parseColumn()
	if !ok {
		return nil, fmt.Errorf("expected column")
	}
	if p.peek().kind == tkLParen {
		cols := []string{col}
		cols = append(cols, p.scanCallCols()...)
		return exprOpaque{Cols: cols}, nil
	}
	if p.eat("is") {
		p.eat("not")
		if !p.eat("null") {
			return nil, fmt.Errorf("expected NULL")
		}
		return exprOpaque{Cols: []string{col}}, nil
	}
	not := false
	if p.eat("not") {
		not = true
	}
	if p.eat("in") {
		return p.parseIn(col, not)
	}
	if p.eat("like") || p.eat("regexp") || p.eat("rlike") {
		if _, err := p.parseValue(); err != nil {
			return nil, err
		}
		return exprOpaque{Cols: []string{col}}, nil
	}
	if not {
		return nil, fmt.Errorf("expected IN after NOT")
	}
	if p.eat("between") {
		if _, err := p.parseValue(); err != nil {
			return nil, err
		}
		if !p.eat("and") {
			return nil, fmt.Errorf("expected AND in BETWEEN")
		}
		if _, err := p.parseValue(); err != nil {
			return nil, err
		}
		return exprOpaque{Cols: []string{col}}, nil
	}
	if p.eat("like") || p.eat("regexp") || p.eat("rlike") {
		if _, err := p.parseValue(); err != nil {
			return nil, err
		}
		return exprOpaque{Cols: []string{col}}, nil
	}
	op, ok := p.eatCompare()
	if !ok {
		return exprOpaque{Cols: []string{col}}, nil
	}
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return exprCmp{Col: col, Op: op, Val: val}, nil
}

func (p *parser) parseIn(col string, not bool) (Expr, error) {
	if p.peek().kind != tkLParen {
		return nil, fmt.Errorf("expected ( after IN")
	}
	p.pop()
	p.depth++
	if p.is("select") {
		return nil, unsupported("subqueries are not supported")
	}
	var vals []Value
	for {
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
		if p.eatComma() {
			continue
		}
		break
	}
	if p.peek().kind != tkRParen {
		return nil, fmt.Errorf("expected ) after IN list")
	}
	p.pop()
	p.depth--
	return exprIn{Col: col, Not: not, Vals: vals}, nil
}

func (p *parser) parseValue() (Value, error) {
	_ = p.eatOp("+")
	if p.peek().kind == tkOp && p.peek().val == "-" {
		p.pop()
		if p.peek().kind != tkNumber {
			return Value{Kind: ValueOther}, nil
		}
		raw := p.pop().val
		if !isDecimalInt(raw) {
			return Value{Kind: ValueOther}, nil
		}
		lit, err := canonicalInt("-" + raw)
		if err != nil {
			return Value{Kind: ValueOther}, nil
		}
		return Value{Kind: ValueLiteral, Literal: lit}, nil
	}
	tok := p.peek()
	switch tok.kind {
	case tkNumber:
		p.pop()
		if strings.HasPrefix(strings.ToLower(tok.val), "0x") || !isDecimalInt(tok.val) {
			return Value{Kind: ValueOther}, nil
		}
		lit, err := canonicalInt(tok.val)
		if err != nil {
			return Value{Kind: ValueOther}, nil
		}
		return Value{Kind: ValueLiteral, Literal: lit}, nil
	case tkString:
		p.pop()
		return Value{Kind: ValueLiteral, Literal: tok.val}, nil
	case tkParam:
		p.pop()
		return Value{Kind: ValuePlaceholder, Index: tok.param}, nil
	case tkIdent:
		switch tok.val {
		case "null":
			p.pop()
			return Value{Kind: ValueNull}, nil
		case "true", "on":
			p.pop()
			return Value{Kind: ValueLiteral, Literal: "1"}, nil
		case "false", "off":
			p.pop()
			return Value{Kind: ValueLiteral, Literal: "0"}, nil
		default:
			p.pop()
			if p.peek().kind == tkLParen {
				_ = p.scanCallCols()
			}
			return Value{Kind: ValueOther}, nil
		}
	case tkLParen:
		_ = p.scanCallCols()
		return Value{Kind: ValueOther}, nil
	default:
		return Value{}, fmt.Errorf("expected value")
	}
}

func (p *parser) parseTuple() ([]Value, error) {
	if p.peek().kind != tkLParen {
		return nil, fmt.Errorf("expected (")
	}
	p.pop()
	p.depth++
	level := p.depth
	var row []Value
	if p.peek().kind == tkRParen && p.depth == level {
		return nil, fmt.Errorf("empty VALUES row")
	}
	for {
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		row = append(row, val)
		if p.peek().kind == tkComma && p.depth == level {
			p.pop()
			continue
		}
		break
	}
	if p.peek().kind != tkRParen || p.depth != level {
		return nil, fmt.Errorf("expected )")
	}
	p.pop()
	p.depth--
	return row, nil
}

func (p *parser) parseIdentList() ([]string, error) {
	if p.peek().kind != tkLParen {
		return nil, fmt.Errorf("expected (")
	}
	p.pop()
	var cols []string
	for {
		if p.peek().kind != tkIdent {
			return nil, fmt.Errorf("expected column name")
		}
		cols = append(cols, p.pop().val)
		if p.eatComma() {
			continue
		}
		break
	}
	if p.peek().kind != tkRParen {
		return nil, fmt.Errorf("expected )")
	}
	p.pop()
	return cols, nil
}

func (p *parser) parseTable() (string, error) {
	if p.peek().kind != tkIdent {
		return "", fmt.Errorf("expected table name")
	}
	name := p.pop().val
	if p.eatDot() {
		if p.peek().kind != tkIdent {
			return "", fmt.Errorf("expected table name")
		}
		name = p.pop().val
	}
	if p.eat("as") {
		if p.peek().kind == tkIdent {
			p.pop()
		}
		return name, nil
	}
	if p.peek().kind == tkIdent && !p.isClauseKeyword(p.peek().val) && !p.isJoinKeyword(p.peek().val) {
		p.pop()
	}
	return name, nil
}

func (p *parser) parseColumn() (string, bool) {
	if p.peek().kind != tkIdent {
		return "", false
	}
	name := p.pop().val
	for p.eatDot() {
		if p.peek().kind != tkIdent {
			return "", false
		}
		name = p.pop().val
	}
	return name, true
}

func (p *parser) rejectJoin() error {
	if p.peek().kind == tkComma || p.isJoinKeyword(p.peek().val) {
		return unsupported("joins and multi-table statements are not supported")
	}
	return nil
}

func (p *parser) skipUntilBoundary(words ...string) error {
	for !p.eof() && !p.isSemi() {
		if p.depth == 0 && p.isAny(words...) {
			return nil
		}
		tok := p.pop()
		if tok.kind == tkLParen {
			p.depth++
		} else if tok.kind == tkRParen {
			if p.depth == 0 {
				return fmt.Errorf("unexpected )")
			}
			p.depth--
		}
	}
	return nil
}

func (p *parser) scanCallCols() []string {
	if p.peek().kind != tkLParen {
		return nil
	}
	p.pop()
	depth := 1
	var cols []string
	for depth > 0 && !p.eof() {
		tok := p.pop()
		switch tok.kind {
		case tkLParen:
			depth++
		case tkRParen:
			depth--
		case tkIdent:
			if depth > 0 {
				cols = append(cols, tok.val)
			}
		}
	}
	return cols
}

func (p *parser) looksLikeColumn() bool {
	if p.peek().kind != tkIdent {
		return false
	}
	switch p.peek().val {
	case "null", "true", "false", "on", "off", "not":
		return false
	default:
		return true
	}
}

func (p *parser) boundary() bool {
	if p.depth != 0 {
		return false
	}
	return p.eof() || p.isSemi() || p.isClauseEnd()
}

func (p *parser) depthAllows() bool {
	return !p.boundary()
}

func (p *parser) isClauseEnd() bool {
	return p.isAny("order", "group", "limit", "having", "for", "lock", "union", "window", "into", "procedure")
}

func (p *parser) isClauseKeyword(s string) bool {
	switch s {
	case "where", "order", "group", "limit", "having", "for", "lock", "union", "window", "into", "set", "values", "value", "on", "procedure", "select":
		return true
	default:
		return false
	}
}

func (p *parser) isJoinKeyword(s string) bool {
	switch s {
	case "join", "inner", "left", "right", "cross", "natural", "straight_join":
		return true
	default:
		return false
	}
}

func isAggregate(name string) bool {
	switch name {
	case "count", "sum", "avg", "min", "max", "group_concat", "json_arrayagg", "json_objectagg", "bit_and", "bit_or", "bit_xor", "std", "stddev", "variance":
		return true
	default:
		return false
	}
}

func isDecimalInt(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (p *parser) eof() bool { return p.peek().kind == tkEOF }

func (p *parser) peek() token {
	if p.i >= len(p.toks) {
		return token{kind: tkEOF}
	}
	return p.toks[p.i]
}

func (p *parser) nextKind(n int) tokenKind {
	i := p.i + n
	if i >= len(p.toks) {
		return tkEOF
	}
	return p.toks[i].kind
}

func (p *parser) pop() token {
	tok := p.peek()
	if tok.kind != tkEOF {
		p.i++
	}
	return tok
}

func (p *parser) is(word string) bool {
	return p.peek().kind == tkIdent && p.peek().val == word
}

func (p *parser) isAny(words ...string) bool {
	if p.peek().kind != tkIdent {
		return false
	}
	for _, w := range words {
		if p.peek().val == w {
			return true
		}
	}
	return false
}

func (p *parser) isSemi() bool { return p.peek().kind == tkSemi }

func (p *parser) eat(word string) bool {
	if p.is(word) {
		p.pop()
		return true
	}
	return false
}

func (p *parser) eatComma() bool {
	if p.peek().kind == tkComma {
		p.pop()
		return true
	}
	return false
}

func (p *parser) eatDot() bool {
	if p.peek().kind == tkDot {
		p.pop()
		return true
	}
	return false
}

func (p *parser) eatOp(op string) bool {
	if p.peek().kind == tkOp && p.peek().val == op {
		p.pop()
		return true
	}
	return false
}

func (p *parser) eatCompare() (string, bool) {
	if p.peek().kind != tkOp {
		return "", false
	}
	switch p.peek().val {
	case "=", "<=>", "<", ">", "<=", ">=", "<>", "!=":
		op := p.pop().val
		if op == "<=>" {
			return "=", true
		}
		return op, true
	default:
		return "", false
	}
}

func (p *parser) skipSemis() {
	for p.isSemi() {
		p.pop()
	}
}

func (p *parser) skipRest() {
	for !p.eof() && !p.isSemi() {
		p.pop()
	}
}

func (p *parser) skipInsertTail() {
	if p.eat("as") {
		if p.peek().kind == tkIdent {
			p.pop()
		}
		if p.peek().kind == tkLParen {
			_ = p.scanCallCols()
		}
	}
	if p.eat("on") {
		p.skipRest()
	}
}

func tokenize(sql string) ([]token, error) {
	var toks []token
	param := 0
	i := 0
	for i < len(sql) {
		r, w := utf8.DecodeRuneInString(sql[i:])
		if r == utf8.RuneError && w == 1 {
			return nil, fmt.Errorf("invalid UTF-8 in SQL")
		}
		if unicode.IsSpace(r) {
			i += w
			continue
		}
		if r == '#' {
			i += w
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if r == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			if i+2 >= len(sql) || sql[i+2] == ' ' || sql[i+2] == '\t' || sql[i+2] == '\n' || sql[i+2] == '\r' {
				i += 2
				for i < len(sql) && sql[i] != '\n' {
					i++
				}
				continue
			}
		}
		if r == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			versioned := false
			if i < len(sql) && sql[i] == '!' {
				versioned = true
				i++
				for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
					i++
				}
			}
			end := strings.Index(sql[i:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated comment")
			}
			inner := sql[i : i+end]
			i += end + 2
			if versioned {
				innerToks, err := tokenize(inner)
				if err != nil {
					return nil, err
				}
				for _, t := range innerToks {
					if t.kind == tkParam {
						t.param = param
						param++
					}
					toks = append(toks, t)
				}
			}
			continue
		}
		switch r {
		case ',':
			toks = append(toks, token{kind: tkComma, val: ","})
			i += w
			continue
		case '(':
			toks = append(toks, token{kind: tkLParen, val: "("})
			i += w
			continue
		case ')':
			toks = append(toks, token{kind: tkRParen, val: ")"})
			i += w
			continue
		case '.':
			toks = append(toks, token{kind: tkDot, val: "."})
			i += w
			continue
		case ';':
			toks = append(toks, token{kind: tkSemi, val: ";"})
			i += w
			continue
		case '?':
			toks = append(toks, token{kind: tkParam, val: "?", param: param})
			param++
			i += w
			continue
		case '\'', '"':
			lit, next, err := scanString(sql, i, byte(r))
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tkString, val: lit})
			i = next
			continue
		case '`':
			lit, next, err := scanIdentQuote(sql, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tkIdent, val: strings.ToLower(lit)})
			i = next
			continue
		case '@':
			name, next := scanVariable(sql, i)
			toks = append(toks, token{kind: tkIdent, val: strings.ToLower(name)})
			i = next
			continue
		}
		if r == '<' || r == '>' || r == '=' || r == '!' || r == '+' || r == '-' || r == '*' || r == '/' || r == '%' {
			op, next := scanOp(sql, i)
			toks = append(toks, token{kind: tkOp, val: op})
			i = next
			continue
		}
		if r == ':' && i+1 < len(sql) && sql[i+1] == '=' {
			toks = append(toks, token{kind: tkOp, val: ":="})
			i += 2
			continue
		}
		if unicode.IsDigit(r) || (r == '0' && i+1 < len(sql) && (sql[i+1] == 'x' || sql[i+1] == 'X')) {
			lit, next := scanNumber(sql, i)
			toks = append(toks, token{kind: tkNumber, val: lit})
			i = next
			continue
		}
		if isIdentStart(r) {
			j := i + w
			for j < len(sql) {
				rr, ww := utf8.DecodeRuneInString(sql[j:])
				if !isIdentCont(rr) {
					break
				}
				j += ww
			}
			toks = append(toks, token{kind: tkIdent, val: strings.ToLower(sql[i:j])})
			i = j
			continue
		}
		return nil, fmt.Errorf("unexpected character %q", string(r))
	}
	return toks, nil
}

func scanOp(sql string, i int) (string, int) {
	if i+2 < len(sql) && sql[i:i+3] == "<=>" {
		return "<=>", i + 3
	}
	if i+1 < len(sql) {
		switch sql[i : i+2] {
		case ">=", "<=", "<>", "!=":
			return sql[i : i+2], i + 2
		}
	}
	return sql[i : i+1], i + 1
}

func scanNumber(sql string, i int) (string, int) {
	j := i
	if sql[j] == '0' && j+1 < len(sql) && (sql[j+1] == 'x' || sql[j+1] == 'X') {
		j += 2
		for j < len(sql) && isHex(sql[j]) {
			j++
		}
		return sql[i:j], j
	}
	for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
		j++
	}
	if j < len(sql) && sql[j] == '.' {
		j++
		for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
			j++
		}
	}
	if j < len(sql) && (sql[j] == 'e' || sql[j] == 'E') {
		k := j + 1
		if k < len(sql) && (sql[k] == '+' || sql[k] == '-') {
			k++
		}
		if k < len(sql) && sql[k] >= '0' && sql[k] <= '9' {
			j = k + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
		}
	}
	return sql[i:j], j
}

func scanString(sql string, i int, quote byte) (string, int, error) {
	var b strings.Builder
	j := i + 1
	for j < len(sql) {
		c := sql[j]
		if c == '\\' && j+1 < len(sql) {
			n := sql[j+1]
			switch n {
			case '0':
				b.WriteByte(0)
			case 'b':
				b.WriteByte('\b')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'Z':
				b.WriteByte(26)
			default:
				b.WriteByte(n)
			}
			j += 2
			continue
		}
		if c == quote {
			if j+1 < len(sql) && sql[j+1] == quote {
				b.WriteByte(quote)
				j += 2
				continue
			}
			return b.String(), j + 1, nil
		}
		b.WriteByte(c)
		j++
	}
	return "", 0, fmt.Errorf("unterminated string")
}

func scanIdentQuote(sql string, i int) (string, int, error) {
	var b strings.Builder
	j := i + 1
	for j < len(sql) {
		if sql[j] == '`' {
			if j+1 < len(sql) && sql[j+1] == '`' {
				b.WriteByte('`')
				j += 2
				continue
			}
			return b.String(), j + 1, nil
		}
		b.WriteByte(sql[j])
		j++
	}
	return "", 0, fmt.Errorf("unterminated identifier")
}

func scanVariable(sql string, i int) (string, int) {
	j := i + 1
	if j < len(sql) && sql[j] == '@' {
		j++
	}
	for j < len(sql) {
		r, w := utf8.DecodeRuneInString(sql[j:])
		if r == '.' || isIdentCont(r) {
			j += w
			continue
		}
		break
	}
	return sql[i:j], j
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_' || r == '$'
}

func isIdentCont(r rune) bool {
	return isIdentStart(r) || unicode.IsDigit(r)
}
