// Package compare provides functionality to compare PostgreSQL database schemas and identify differences
// between them. It can detect differences in tables, columns, primary keys, indexes, and foreign keys.
package compare

import (
	"fmt"
	"strings"

	"github.com/agustin/postgres_schema_check/pkg/schema"
)

// Difference represents a single difference found between two database schemas.
// It includes the type of difference, the affected table, and a human-readable description.
type Difference struct {
	Type        string // Type of difference (e.g., "MissingTable", "ColumnTypeMismatch")
	Table       string // Name of the table where the difference was found
	Description string // Human-readable description of the difference
}

// CompareSchemas performs a comprehensive comparison between two database schemas.
// It checks for differences in tables, columns, primary keys, indexes, and foreign keys.
//
// Parameters:
//   - source: The source schema to compare from
//   - target: The target schema to compare against
//
// Returns:
//   - []Difference: A list of all differences found between the schemas
func CompareSchemas(source, target *schema.Schema) []Difference {
	var differences []Difference

	// Compare tables that exist in the source schema
	for tableName, sourceTable := range source.Tables {
		targetTable, exists := target.Tables[tableName]
		if !exists {
			differences = append(differences, Difference{
				Type:        "MissingTable",
				Table:       tableName,
				Description: "Table exists in source but not in target",
			})
			continue
		}

		// Compare all aspects of the table
		columnDiffs := compareColumns(tableName, sourceTable.Columns, targetTable.Columns)
		differences = append(differences, columnDiffs...)

		pkDiffs := comparePrimaryKeys(tableName, sourceTable.PrimaryKeys, targetTable.PrimaryKeys)
		differences = append(differences, pkDiffs...)

		indexDiffs := compareIndexes(tableName, sourceTable.Indexes, targetTable.Indexes)
		differences = append(differences, indexDiffs...)

		fkDiffs := compareForeignKeys(tableName, sourceTable.ForeignKeys, targetTable.ForeignKeys)
		differences = append(differences, fkDiffs...)
	}

	// Check for tables that exist only in the target schema
	for tableName := range target.Tables {
		if _, exists := source.Tables[tableName]; !exists {
			differences = append(differences, Difference{
				Type:        "ExtraTable",
				Table:       tableName,
				Description: "Table exists in target but not in source",
			})
		}
	}

	return differences
}

// compareColumns compares the columns of a table between source and target schemas.
// It checks for missing columns, type mismatches, nullability differences,
// default value differences, and identity column differences.
//
// Parameters:
//   - tableName: Name of the table being compared
//   - source: List of columns in the source schema
//   - target: List of columns in the target schema
//
// Returns:
//   - []Difference: List of differences found in the columns
func compareColumns(tableName string, source, target []schema.ColumnInfo) []Difference {
	var differences []Difference
	sourceMap := make(map[string]schema.ColumnInfo)
	targetMap := make(map[string]schema.ColumnInfo)

	// Create maps for efficient column lookup
	for _, col := range source {
		sourceMap[col.Name] = col
	}
	for _, col := range target {
		targetMap[col.Name] = col
	}

	// Check for missing or different columns in source
	for name, sourceCol := range sourceMap {
		targetCol, exists := targetMap[name]
		if !exists {
			differences = append(differences, Difference{
				Type:        "MissingColumn",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' exists in source but not in target", name),
			})
			continue
		}

		// Compare column properties
		if sourceCol.Type != targetCol.Type {
			differences = append(differences, Difference{
				Type:        "ColumnTypeMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' has different types: source=%s, target=%s", name, sourceCol.Type, targetCol.Type),
			})
		}

		if sourceCol.Nullable != targetCol.Nullable {
			differences = append(differences, Difference{
				Type:        "ColumnNullableMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' has different nullable settings: source=%v, target=%v", name, sourceCol.Nullable, targetCol.Nullable),
			})
		}

		if sourceCol.Default != targetCol.Default {
			differences = append(differences, Difference{
				Type:        "ColumnDefaultMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' has different default values: source=%s, target=%s", name, sourceCol.Default, targetCol.Default),
			})
		}

		if sourceCol.IsIdentity != targetCol.IsIdentity {
			differences = append(differences, Difference{
				Type:        "ColumnIdentityMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' has different identity settings: source=%v, target=%v", name, sourceCol.IsIdentity, targetCol.IsIdentity),
			})
		}
	}

	// Check for extra columns in target
	for name := range targetMap {
		if _, exists := sourceMap[name]; !exists {
			differences = append(differences, Difference{
				Type:        "ExtraColumn",
				Table:       tableName,
				Description: fmt.Sprintf("Column '%s' exists in target but not in source", name),
			})
		}
	}

	return differences
}

// comparePrimaryKeys compares the primary key definitions between source and target schemas.
// It checks for differences in the number of primary key columns and their order.
//
// Parameters:
//   - tableName: Name of the table being compared
//   - source: List of primary key columns in the source schema
//   - target: List of primary key columns in the target schema
//
// Returns:
//   - []Difference: List of differences found in the primary keys
func comparePrimaryKeys(tableName string, source, target []string) []Difference {
	var differences []Difference

	// Check if the number of primary key columns matches
	if len(source) != len(target) {
		differences = append(differences, Difference{
			Type:        "PrimaryKeyMismatch",
			Table:       tableName,
			Description: fmt.Sprintf("Different number of primary key columns: source=%d, target=%d", len(source), len(target)),
		})
		return differences
	}

	// Compare each primary key column in order
	for i := range source {
		if source[i] != target[i] {
			differences = append(differences, Difference{
				Type:        "PrimaryKeyMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Primary key column mismatch at position %d: source=%s, target=%s", i+1, source[i], target[i]),
			})
		}
	}

	return differences
}

// compareIndexes compares the indexes between source and target schemas.
// It checks for missing indexes, uniqueness differences, and column differences.
//
// Parameters:
//   - tableName: Name of the table being compared
//   - source: List of indexes in the source schema
//   - target: List of indexes in the target schema
//
// Returns:
//   - []Difference: List of differences found in the indexes
func compareIndexes(tableName string, source, target []schema.IndexInfo) []Difference {
	var differences []Difference
	sourceMap := make(map[string]schema.IndexInfo)
	targetMap := make(map[string]schema.IndexInfo)

	// Create maps for efficient index lookup
	for _, idx := range source {
		sourceMap[idx.Name] = idx
	}
	for _, idx := range target {
		targetMap[idx.Name] = idx
	}

	// Check for missing or different indexes in source
	for name, sourceIdx := range sourceMap {
		targetIdx, exists := targetMap[name]
		if !exists {
			differences = append(differences, Difference{
				Type:        "MissingIndex",
				Table:       tableName,
				Description: fmt.Sprintf("Index '%s' exists in source but not in target", name),
			})
			continue
		}

		// Compare index properties
		if sourceIdx.Unique != targetIdx.Unique {
			differences = append(differences, Difference{
				Type:        "IndexUniqueMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Index '%s' has different unique settings: source=%v, target=%v", name, sourceIdx.Unique, targetIdx.Unique),
			})
		}

		if !compareStringSlices(sourceIdx.Columns, targetIdx.Columns) {
			differences = append(differences, Difference{
				Type:        "IndexColumnsMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Index '%s' has different columns: source=%v, target=%v", name, sourceIdx.Columns, targetIdx.Columns),
			})
		}
	}

	// Check for extra indexes in target
	for name := range targetMap {
		if _, exists := sourceMap[name]; !exists {
			differences = append(differences, Difference{
				Type:        "ExtraIndex",
				Table:       tableName,
				Description: fmt.Sprintf("Index '%s' exists in target but not in source", name),
			})
		}
	}

	return differences
}

// compareForeignKeys compares the foreign key constraints between source and target schemas.
// It checks for missing foreign keys, referenced table differences, and column differences.
//
// Parameters:
//   - tableName: Name of the table being compared
//   - source: List of foreign keys in the source schema
//   - target: List of foreign keys in the target schema
//
// Returns:
//   - []Difference: List of differences found in the foreign keys
func compareForeignKeys(tableName string, source, target []schema.ForeignKeyInfo) []Difference {
	var differences []Difference
	sourceMap := make(map[string]schema.ForeignKeyInfo)
	targetMap := make(map[string]schema.ForeignKeyInfo)

	// Create maps for efficient foreign key lookup
	for _, fk := range source {
		sourceMap[fk.Name] = fk
	}
	for _, fk := range target {
		targetMap[fk.Name] = fk
	}

	// Check for missing or different foreign keys in source
	for name, sourceFK := range sourceMap {
		targetFK, exists := targetMap[name]
		if !exists {
			differences = append(differences, Difference{
				Type:        "MissingForeignKey",
				Table:       tableName,
				Description: fmt.Sprintf("Foreign key '%s' exists in source but not in target", name),
			})
			continue
		}

		// Compare foreign key properties
		if sourceFK.ReferencedTable != targetFK.ReferencedTable {
			differences = append(differences, Difference{
				Type:        "ForeignKeyReferenceMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Foreign key '%s' references different tables: source=%s, target=%s", name, sourceFK.ReferencedTable, targetFK.ReferencedTable),
			})
		}

		if !compareStringSlices(sourceFK.Columns, targetFK.Columns) {
			differences = append(differences, Difference{
				Type:        "ForeignKeyColumnsMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Foreign key '%s' has different columns: source=%v, target=%v", name, sourceFK.Columns, targetFK.Columns),
			})
		}

		if !compareStringSlices(sourceFK.ReferencedColumns, targetFK.ReferencedColumns) {
			differences = append(differences, Difference{
				Type:        "ForeignKeyReferencedColumnsMismatch",
				Table:       tableName,
				Description: fmt.Sprintf("Foreign key '%s' references different columns: source=%v, target=%v", name, sourceFK.ReferencedColumns, targetFK.ReferencedColumns),
			})
		}
	}

	// Check for extra foreign keys in target
	for name := range targetMap {
		if _, exists := sourceMap[name]; !exists {
			differences = append(differences, Difference{
				Type:        "ExtraForeignKey",
				Table:       tableName,
				Description: fmt.Sprintf("Foreign key '%s' exists in target but not in source", name),
			})
		}
	}

	return differences
}

// compareStringSlices compares two string slices for equality.
// The order of elements matters in the comparison.
//
// Parameters:
//   - a: First string slice to compare
//   - b: Second string slice to compare
//
// Returns:
//   - bool: True if the slices are equal, false otherwise
func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
} 