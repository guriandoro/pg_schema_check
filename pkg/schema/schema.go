// Package schema provides functionality to interact with and analyze PostgreSQL database schemas.
// It includes types and functions to fetch and represent table structures, columns, indexes, and constraints.
package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// TableInfo represents the complete structure of a PostgreSQL table, including its columns,
// primary keys, indexes, and foreign key relationships.
type TableInfo struct {
	Name        string            // Name of the table
	Columns     []ColumnInfo      // List of columns in the table
	PrimaryKeys []string          // Names of columns that form the primary key
	Indexes     []IndexInfo       // List of indexes defined on the table
	ForeignKeys []ForeignKeyInfo  // List of foreign key constraints
}

// ColumnInfo represents a single column in a PostgreSQL table, including its data type,
// nullability, default value, and identity status.
type ColumnInfo struct {
	Name       string // Name of the column
	Type       string // PostgreSQL data type of the column
	Nullable   bool   // Whether the column can contain NULL values
	Default    string // Default value expression for the column
	IsIdentity bool   // Whether the column is an identity column (auto-incrementing)
}

// IndexInfo represents a database index, including its name, the columns it covers,
// and whether it enforces uniqueness.
type IndexInfo struct {
	Name    string   // Name of the index
	Columns []string // Names of columns included in the index
	Unique  bool     // Whether the index enforces uniqueness
}

// ForeignKeyInfo represents a foreign key constraint that links columns in one table
// to columns in another table.
type ForeignKeyInfo struct {
	Name              string   // Name of the foreign key constraint
	Columns           []string // Names of columns in the current table
	ReferencedTable   string   // Name of the table being referenced
	ReferencedColumns []string // Names of columns in the referenced table
}

// Schema represents a complete database schema, containing all tables and their relationships.
type Schema struct {
	Tables map[string]TableInfo // Map of table names to their complete information
}

// NewSchema creates and returns a new empty Schema instance.
// It initializes the Tables map to be ready for use.
func NewSchema() *Schema {
	return &Schema{
		Tables: make(map[string]TableInfo),
	}
}

// FetchSchema retrieves the complete schema information from a PostgreSQL database.
// It queries the information_schema to get details about all tables, their columns,
// constraints, and relationships.
//
// Parameters:
//   - ctx: Context for the database operation
//   - conn: Active PostgreSQL connection
//
// Returns:
//   - *Schema: Complete schema information
//   - error: Any error that occurred during the fetch operation
func FetchSchema(ctx context.Context, conn *pgx.Conn) (*Schema, error) {
	schema := NewSchema()

	// Query to fetch all table names from the public schema
	rows, err := conn.Query(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public'
		ORDER BY table_name
	`)
	if err != nil {
		return nil, fmt.Errorf("error fetching tables: %w", err)
	}
	defer rows.Close()

	// Collect all table names first
	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error scanning table name: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}

	// Check for any errors that occurred during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table names: %w", err)
	}

	// Now that the initial query is complete, fetch detailed info for each table
	for _, tableName := range tableNames {
		tableInfo, err := fetchTableInfo(ctx, conn, tableName)
		if err != nil {
			return nil, fmt.Errorf("error fetching table info for %s: %w", tableName, err)
		}

		schema.Tables[tableName] = tableInfo
	}

	return schema, nil
}

// fetchTableInfo retrieves detailed information about a specific table, including its columns,
// primary keys, indexes, and foreign key constraints.
//
// Parameters:
//   - ctx: Context for the database operation
//   - conn: Active PostgreSQL connection
//   - tableName: Name of the table to fetch information for
//
// Returns:
//   - TableInfo: Complete information about the table
//   - error: Any error that occurred during the fetch operation
func fetchTableInfo(ctx context.Context, conn *pgx.Conn, tableName string) (TableInfo, error) {
	tableInfo := TableInfo{
		Name: tableName,
	}

	// Fetch column information including data types, nullability, defaults, and identity status
	rows, err := conn.Query(ctx, `
		SELECT 
			column_name,
			data_type,
			is_nullable,
			column_default,
			is_identity
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		return tableInfo, fmt.Errorf("error fetching columns: %w", err)
	}
	defer rows.Close()

	// Process each column and add it to the table information
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		var defaultVal sql.NullString
		var identity string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &defaultVal, &identity); err != nil {
			return tableInfo, fmt.Errorf("error scanning column: %w", err)
		}
		col.Nullable = nullable == "YES"
		col.IsIdentity = identity == "YES"
		if defaultVal.Valid {
			col.Default = defaultVal.String
		} else {
			col.Default = ""
		}
		tableInfo.Columns = append(tableInfo.Columns, col)
	}

	// Check for any errors that occurred during iteration
	if err := rows.Err(); err != nil {
		return tableInfo, fmt.Errorf("error iterating columns: %w", err)
	}

	// Fetch primary key information
	pkRows, err := conn.Query(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = 'public'
			AND tc.table_name = $1
		ORDER BY kcu.ordinal_position
	`, tableName)
	if err != nil {
		return tableInfo, fmt.Errorf("error fetching primary keys: %w", err)
	}
	defer pkRows.Close()

	// Process each primary key column
	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return tableInfo, fmt.Errorf("error scanning primary key: %w", err)
		}
		tableInfo.PrimaryKeys = append(tableInfo.PrimaryKeys, colName)
	}

	// Check for any errors that occurred during iteration
	if err := pkRows.Err(); err != nil {
		return tableInfo, fmt.Errorf("error iterating primary keys: %w", err)
	}

	// Fetch index information including index names, columns, and uniqueness
	indexRows, err := conn.Query(ctx, `
		SELECT
			i.relname as index_name,
			array_agg(a.attname) as column_names,
			ix.indisunique as is_unique
		FROM
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute a
		WHERE
			t.oid = ix.indrelid
			AND i.oid = ix.indexrelid
			AND a.attrelid = t.oid
			AND a.attnum = ANY(ix.indkey)
			AND t.relkind = 'r'
			AND t.relname = $1
		GROUP BY
			i.relname,
			ix.indisunique
		ORDER BY
			i.relname
	`, tableName)
	if err != nil {
		return tableInfo, fmt.Errorf("error fetching indexes: %w", err)
	}
	defer indexRows.Close()

	// Process each index
	for indexRows.Next() {
		var idx IndexInfo
		if err := indexRows.Scan(&idx.Name, &idx.Columns, &idx.Unique); err != nil {
			return tableInfo, fmt.Errorf("error scanning index: %w", err)
		}
		tableInfo.Indexes = append(tableInfo.Indexes, idx)
	}

	// Check for any errors that occurred during iteration
	if err := indexRows.Err(); err != nil {
		return tableInfo, fmt.Errorf("error iterating indexes: %w", err)
	}

	// Fetch foreign key information including referenced tables and columns
	fkRows, err := conn.Query(ctx, `
		SELECT
			tc.constraint_name,
			array_agg(kcu.column_name) as columns,
			ccu.table_name as referenced_table,
			array_agg(ccu.column_name) as referenced_columns
		FROM
			information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name
		WHERE
			tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = 'public'
			AND tc.table_name = $1
		GROUP BY
			tc.constraint_name,
			ccu.table_name
	`, tableName)
	if err != nil {
		return tableInfo, fmt.Errorf("error fetching foreign keys: %w", err)
	}
	defer fkRows.Close()

	// Process each foreign key constraint
	for fkRows.Next() {
		var fk ForeignKeyInfo
		if err := fkRows.Scan(&fk.Name, &fk.Columns, &fk.ReferencedTable, &fk.ReferencedColumns); err != nil {
			return tableInfo, fmt.Errorf("error scanning foreign key: %w", err)
		}
		tableInfo.ForeignKeys = append(tableInfo.ForeignKeys, fk)
	}

	// Check for any errors that occurred during iteration
	if err := fkRows.Err(); err != nil {
		return tableInfo, fmt.Errorf("error iterating foreign keys: %w", err)
	}

	return tableInfo, nil
} 