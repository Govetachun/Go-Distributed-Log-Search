package args

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// parseDuration parses a human-readable duration string
func parseDuration(s string) (time.Duration, error) {
	duration, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration: %s. Please use Go duration format (e.g., '30s', '5m', '1h')", err)
	}
	return duration, nil
}

// Args represents the main command line arguments
type Args struct {
	DB     string     `json:"db"`
	SubCmd SubCommand `json:"subcmd"`
}

// SubCommand represents the subcommands available
type SubCommand struct {
	Name       string      `json:"name"`
	CreateArgs *CreateArgs `json:"create_args,omitempty"`
	DropArgs   *DropArgs   `json:"drop_args,omitempty"`
	IndexArgs  *IndexArgs  `json:"index_args,omitempty"`
	MergeArgs  *MergeArgs  `json:"merge_args,omitempty"`
	SearchArgs *SearchArgs `json:"search_args,omitempty"`
}

// CreateArgs represents arguments for the create subcommand
type CreateArgs struct {
	ConfigPath string `json:"config_path"`
}

// DropArgs represents arguments for the drop subcommand
type DropArgs struct {
	Name string `json:"name"`
}

// IndexArgs represents arguments for the index subcommand
type IndexArgs struct {
	Name           string        `json:"name"`
	Input          string        `json:"input"`
	Stream         bool          `json:"stream"`
	CommitInterval time.Duration `json:"commit_interval"`
	BuildDir       string        `json:"build_dir"`
	MemoryBudget   int           `json:"memory_budget"`
}

// MergeArgs represents arguments for the merge subcommand
type MergeArgs struct {
	Name     string `json:"name"`
	MergeDir string `json:"merge_dir"`
}

// SearchArgs represents arguments for the search subcommand
type SearchArgs struct {
	Name  string `json:"name"`
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

// Global variables to store parsed arguments
var (
	globalArgs Args
	rootCmd    *cobra.Command
)

// createRootCmd creates the root command
func createRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "toshokan",
		Short: "A search engine on object storage",
		Long:  "A search engine on object storage - Go implementation",
	}

	// Add global flags
	cmd.PersistentFlags().StringVar(&globalArgs.DB, "db", "",
		"Postgres DB connection url. Can also be provided by a DATABASE_URL env var, but only if this arg is not provided.")

	return cmd
}

// createCreateCmd creates the create subcommand
func createCreateCmd() *cobra.Command {
	createArgs := &CreateArgs{}

	cmd := &cobra.Command{
		Use:   "create [config_path]",
		Short: "Create a new index",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			createArgs.ConfigPath = args[0]
			globalArgs.SubCmd = SubCommand{
				Name:       "create",
				CreateArgs: createArgs,
			}
		},
	}

	return cmd
}

// createDropCmd creates the drop subcommand
func createDropCmd() *cobra.Command {
	dropArgs := &DropArgs{}

	cmd := &cobra.Command{
		Use:   "drop [name]",
		Short: "Drop an index",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			dropArgs.Name = args[0]
			globalArgs.SubCmd = SubCommand{
				Name:     "drop",
				DropArgs: dropArgs,
			}
		},
	}

	return cmd
}

// createIndexCmd creates the index subcommand
func createIndexCmd() *cobra.Command {
	indexArgs := &IndexArgs{
		Stream:         false,
		CommitInterval: 30 * time.Second,
		BuildDir:       "/tmp/toshokan_build",
		MemoryBudget:   1073741824, // 1GB
	}

	cmd := &cobra.Command{
		Use:   "index [name] [input]",
		Short: "Index documents",
		Long: `Index documents from a JSONL file.
Read from stdin by not providing any file path.`,
		Args: cobra.RangeArgs(1, 2),
		Run: func(cmd *cobra.Command, args []string) {
			indexArgs.Name = args[0]
			if len(args) > 1 {
				indexArgs.Input = args[1]
			}
			globalArgs.SubCmd = SubCommand{
				Name:      "index",
				IndexArgs: indexArgs,
			}
		},
	}

	// Add flags specific to index command
	cmd.Flags().BoolVarP(&indexArgs.Stream, "stream", "s", false,
		"Whether to stream from the source without terminating. Will stop only once the source is closed.")

	var commitIntervalStr string
	cmd.Flags().StringVar(&commitIntervalStr, "commit-interval", "30s",
		"How much time to collect docs from the source until an index file should be generated. Only used when streaming. Examples: '5s', '2m10s'.")

	cmd.Flags().StringVarP(&indexArgs.BuildDir, "build-dir", "b", "/tmp/toshokan_build",
		"Path to the dir to build in the inverted indexes.")

	cmd.Flags().IntVar(&indexArgs.MemoryBudget, "memory-budget", 1073741824,
		"Sets the amount of memory allocated for all indexing threads. The memory is split evenly between all indexing threads, once a thread reaches its limit a commit is triggered.")

	// Parse commit interval in PreRun
	cmd.PreRun = func(cmd *cobra.Command, args []string) {
		if commitIntervalStr != "" {
			duration, err := parseDuration(commitIntervalStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing commit-interval: %v\n", err)
				os.Exit(1)
			}
			indexArgs.CommitInterval = duration
		}
	}

	return cmd
}

// createMergeCmd creates the merge subcommand
func createMergeCmd() *cobra.Command {
	mergeArgs := &MergeArgs{
		MergeDir: "/tmp/toshokan_merge",
	}

	cmd := &cobra.Command{
		Use:   "merge [name]",
		Short: "Merge index segments",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			mergeArgs.Name = args[0]
			globalArgs.SubCmd = SubCommand{
				Name:      "merge",
				MergeArgs: mergeArgs,
			}
		},
	}

	// Add flags specific to merge command
	cmd.Flags().StringVarP(&mergeArgs.MergeDir, "merge-dir", "m", "/tmp/toshokan_merge",
		"Path to the dir to merge in the inverted indexes.")

	return cmd
}

// createSearchCmd creates the search subcommand
func createSearchCmd() *cobra.Command {
	searchArgs := &SearchArgs{
		Limit: 1,
	}

	cmd := &cobra.Command{
		Use:   "search [name] [query]",
		Short: "Search the index",
		Long:  "Search the index using Tantivy query syntax.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			searchArgs.Name = args[0]
			searchArgs.Query = args[1]
			globalArgs.SubCmd = SubCommand{
				Name:       "search",
				SearchArgs: searchArgs,
			}
		},
	}

	// Add flags specific to search command
	cmd.Flags().IntVarP(&searchArgs.Limit, "limit", "l", 1,
		"Limit to a number of top results.")

	return cmd
}

// ParseArgs parses command line arguments and returns Args struct
func ParseArgs() (*Args, error) {
	// Initialize global args
	globalArgs = Args{}

	// Create root command
	rootCmd = createRootCmd()

	// Add subcommands
	rootCmd.AddCommand(createCreateCmd())
	rootCmd.AddCommand(createDropCmd())
	rootCmd.AddCommand(createIndexCmd())
	rootCmd.AddCommand(createMergeCmd())
	rootCmd.AddCommand(createSearchCmd())

	// Execute command parsing
	if err := rootCmd.Execute(); err != nil {
		return nil, fmt.Errorf("failed to parse arguments: %w", err)
	}

	return &globalArgs, nil
}
