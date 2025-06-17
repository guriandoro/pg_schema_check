// Package main implements a command-line tool for comparing PostgreSQL database schemas.
// It provides functionality to connect to two databases, fetch their schemas,
// and report any differences found between them.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/agustin/postgres_schema_check/pkg/compare"
	"github.com/agustin/postgres_schema_check/pkg/schema"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

// Global variables for command-line flags
var (
	sourceConnString string // Connection string for the source database
	targetConnString string // Connection string for the target database
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "schema-check",
	Short: "Compare PostgreSQL database schemas",
	Long:  `A tool to compare the schema of two PostgreSQL databases and report differences.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a background context for database operations
		ctx := context.Background()

		// Connect to source database
		sourceConn, err := pgx.Connect(ctx, sourceConnString)
		if err != nil {
			return fmt.Errorf("error connecting to source database: %w", err)
		}
		defer sourceConn.Close(ctx)

		// Connect to target database
		targetConn, err := pgx.Connect(ctx, targetConnString)
		if err != nil {
			return fmt.Errorf("error connecting to target database: %w", err)
		}
		defer targetConn.Close(ctx)

		// Fetch schema information from both databases
		sourceSchema, err := schema.FetchSchema(ctx, sourceConn)
		if err != nil {
			return fmt.Errorf("error fetching source schema: %w", err)
		}

		targetSchema, err := schema.FetchSchema(ctx, targetConn)
		if err != nil {
			return fmt.Errorf("error fetching target schema: %w", err)
		}

		// Compare the schemas and get a list of differences
		differences := compare.CompareSchemas(sourceSchema, targetSchema)

		// Print the results
		if len(differences) == 0 {
			fmt.Println("No differences found between the schemas.")
			return nil
		}

		fmt.Printf("Found %d differences:\n\n", len(differences))
		for _, diff := range differences {
			fmt.Printf("[%s] %s: %s\n", diff.Type, diff.Table, diff.Description)
		}

		return nil
	},
}

// init initializes the command-line flags and marks them as required
func init() {
	// Define command-line flags
	rootCmd.Flags().StringVar(&sourceConnString, "source", "", "Source database connection string")
	rootCmd.Flags().StringVar(&targetConnString, "target", "", "Target database connection string")
	
	// Mark flags as required
	rootCmd.MarkFlagRequired("source")
	rootCmd.MarkFlagRequired("target")
}

// main is the entry point of the application
func main() {
	// Execute the root command and handle any errors
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
} 