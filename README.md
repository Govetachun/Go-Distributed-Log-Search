# Toshokan Go

A **Go implementation** of the [Toshokan](https://github.com/tontinton/toshokan) search engine for object storage.  
This project mirrors the structure and functionality of the original Rust implementation, while embracing Go idioms and tooling.
```
toshokan-go/
├── src/
│   ├── main.go           # Main entry point (equivalent to main.rs)
│   ├── args/
│   │   └── args.go       # CLI argument parsing (equivalent to args.rs)
│   └── commands/
│       └── commands.go   # Command implementations (equivalent to commands/*.rs)
├── migrations/           # Database migrations
├── tests/               # Test files
├── go.mod               # Go module file
└── README.md            # This file
```
## 📖 Reference

This project is a Go port of the original Rust implementation:
👉 [tontinton/toshokan](https://github.com/tontinton/toshokan)