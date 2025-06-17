# PostgreSQL Schema Check

A command-line tool to compare the schema of two PostgreSQL databases and report differences. This tool helps you identify schema discrepancies between databases, which is particularly useful for:

- Verifying database migrations
- Ensuring development and production schemas match
- Validating database backups
- Checking schema consistency across environments

## Features

- Compares table structures
- Identifies missing or extra tables
- Compares column definitions (type, nullable, default values, identity)
- Compares primary keys
- Compares indexes
- Compares foreign key constraints
- Detailed difference reporting

## Installation

1. Install Go 1.21 or later
2. Clone this repository
3. Build the tool:

```bash
go build -o schema-check ./cmd/schema-check
```

## Usage

```bash
./schema-check --source "postgresql://user:password@localhost:5432/source_db" --target "postgresql://user:password@localhost:5432/target_db"
```

### Connection String Format

The connection string should follow the PostgreSQL connection string format:
```postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
```

### Example Output

```
Found 3 differences:

[MissingColumn] users: Column 'last_login' exists in source but not in target
[ColumnTypeMismatch] products: Column 'price' has different types: source=numeric, target=integer
[ExtraIndex] orders: Index 'idx_order_date' exists in target but not in source
```

## Development

### Dependency Management

If you encounter errors about missing `go.sum` entries for dependencies like `github.com/jackc/pgx/v5` or `github.com/spf13/cobra`, run the following command to fetch all required dependencies and update your `go.mod` and `go.sum` files:

```bash
go mod tidy
```

This will:
- Download any missing dependencies
- Add the necessary entries to your `go.sum` file
- Clean up any unused dependencies

### Project Structure

```
.
├── cmd/
│   └── schema-check/    # Command-line interface
├── pkg/
│   ├── schema/         # Schema extraction and representation
│   └── compare/        # Schema comparison logic
└── README.md
```

### Building from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/postgres_schema_check.git
cd postgres_schema_check

# Build the tool
go build -o schema-check ./cmd/schema-check
```

## License

MIT License 