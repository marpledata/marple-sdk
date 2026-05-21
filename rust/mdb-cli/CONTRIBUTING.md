# Contributing to `mdb`

## Development Setup

### Prerequisites

- Rust 1.70 or later (install via [rustup](https://rustup.rs/))
- A MarpleDB API token (for running integration tests)

### Getting Started

1. Fork and clone the repository:

   ```sh
   git clone https://github.com/marpledata/marple-sdk.git
   cd marple-sdk/mdb-cli
   ```

2. Build the project:

   ```sh
   cargo build
   ```

3. Run the CLI:
   ```sh
   cargo run -- --help
   ```

## Development Workflow

### Common commands

Build in debug mode (faster compilation):

```sh
cargo build
cargo build --release
export MDB_TOKEN="your_token_here"
cargo test
```

## Questions?

If you have questions or need help, please:

- Open an issue for discussion
- Check existing issues and PRs
- Review the codebase for examples

Thank you for contributing!
