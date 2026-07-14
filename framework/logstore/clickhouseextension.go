package logstore

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var clickHouseExtensionTableNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var (
	clickHouseExtensionExpressionPattern = regexp.MustCompile(`^[A-Za-z0-9_(),.+*/%<>=! \t-]+$`)
	clickHouseExtensionDDLKeywordPattern = regexp.MustCompile(`(?i)\b(?:ALTER|ATTACH|CREATE|DELETE|DETACH|DROP|ENGINE|FORMAT|GRANT|INSERT|INTO|ORDER|PARTITION|RENAME|REVOKE|SELECT|SETTINGS|SYSTEM|TRUNCATE|TTL|UNION|UPDATE)\b`)
	clickHouseExtensionIndexPattern      = regexp.MustCompile(`(?i)^INDEX [A-Za-z_][A-Za-z0-9_]* [A-Za-z_][A-Za-z0-9_]* TYPE (?:bloom_filter|set\([1-9][0-9]*\)) GRANULARITY [1-9][0-9]*$`)
)

var clickHouseCoreTableNames = map[string]struct{}{
	"async_jobs":    {},
	"logs":          {},
	"mcp_tool_logs": {},
}

// ClickHouseExtensionTableOptions defines the trusted schema shape for an
// extension-owned table. DDL expressions must not be populated from user input.
type ClickHouseExtensionTableOptions struct {
	Table       string
	PartitionBy string
	OrderBy     string
	TTL         string
	SkipIndexes []string
}

type clickHouseSchemaStore interface {
	EnsureClickHouseTable(ctx context.Context, model any, opts ClickHouseExtensionTableOptions) error
}

var (
	_ clickHouseSchemaStore = (*ClickHouseLogStore)(nil)
	_ clickHouseSchemaStore = (*HybridLogStore)(nil)
)

// EnsureClickHouseTable creates an extension table and reconciles newly added
// model columns. It preserves the configured cluster and replication settings.
func (s *ClickHouseLogStore) EnsureClickHouseTable(ctx context.Context, model any, opts ClickHouseExtensionTableOptions) error {
	if s == nil || s.RDBLogStore == nil || s.db == nil {
		return fmt.Errorf("clickhouse: logstore is not initialized")
	}
	if err := validateClickHouseExtensionTableOptions(opts); err != nil {
		return err
	}

	tableOpts := chTableOpts{
		table:       opts.Table,
		partitionBy: opts.PartitionBy,
		orderBy:     opts.OrderBy,
		ttl:         opts.TTL,
		skipIndexes: append([]string(nil), opts.SkipIndexes...),
	}
	if err := clickhouseCreateTable(ctx, s.db, model, tableOpts, s.cluster); err != nil {
		return fmt.Errorf("clickhouse: create extension table %s: %w", opts.Table, err)
	}
	return clickhouseReconcileColumns(ctx, s.db, model, opts.Table, s.cluster, s.logger)
}

// EnsureClickHouseTable delegates extension-table schema management to a
// ClickHouse logstore wrapped by hybrid object storage.
func (h *HybridLogStore) EnsureClickHouseTable(ctx context.Context, model any, opts ClickHouseExtensionTableOptions) error {
	schemaStore, ok := h.inner.(clickHouseSchemaStore)
	if !ok {
		return fmt.Errorf("logstore does not support ClickHouse extension tables")
	}
	return schemaStore.EnsureClickHouseTable(ctx, model, opts)
}

// validateClickHouseExtensionTableOptions rejects core-table collisions and
// limits DDL fragments to the expression/index grammar used by extension tables.
func validateClickHouseExtensionTableOptions(opts ClickHouseExtensionTableOptions) error {
	if !clickHouseExtensionTableNamePattern.MatchString(opts.Table) {
		return fmt.Errorf("clickhouse: invalid extension table name %q", opts.Table)
	}
	if _, reserved := clickHouseCoreTableNames[strings.ToLower(opts.Table)]; reserved {
		return fmt.Errorf("clickhouse: extension table name %q is reserved", opts.Table)
	}
	if err := validateClickHouseExtensionExpression("partition by", opts.PartitionBy, false); err != nil {
		return err
	}
	if err := validateClickHouseExtensionExpression("order by", opts.OrderBy, true); err != nil {
		return err
	}
	if err := validateClickHouseExtensionExpression("ttl", opts.TTL, false); err != nil {
		return err
	}
	for _, index := range opts.SkipIndexes {
		if !clickHouseExtensionIndexPattern.MatchString(strings.TrimSpace(index)) {
			return fmt.Errorf("clickhouse: invalid extension table index %q", index)
		}
	}
	return nil
}

// validateClickHouseExtensionExpression accepts balanced scalar expressions
// while rejecting statement-level DDL/DML tokens and comment delimiters.
func validateClickHouseExtensionExpression(name string, expression string, required bool) error {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		if required {
			return fmt.Errorf("clickhouse: extension table %s is required", name)
		}
		return nil
	}
	if !clickHouseExtensionExpressionPattern.MatchString(expression) ||
		strings.Contains(expression, "--") || strings.Contains(expression, "/*") || strings.Contains(expression, "*/") ||
		clickHouseExtensionDDLKeywordPattern.MatchString(expression) {
		return fmt.Errorf("clickhouse: invalid extension table %s expression %q", name, expression)
	}
	depth := 0
	for _, char := range expression {
		switch char {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("clickhouse: invalid extension table %s expression %q", name, expression)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("clickhouse: invalid extension table %s expression %q", name, expression)
	}
	return nil
}
