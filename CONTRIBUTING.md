# Contributing

Thanks for your interest in Twilight AI. Bug reports, fixes, and new providers are welcome.

For security issues, do not open a public issue. See [SECURITY.md](SECURITY.md).

## Development environment

You need:

- Go at the version declared in [`go.mod`](go.mod).
- [golangci-lint](https://golangci-lint.run/) at the version pinned in [`mise.toml`](mise.toml).

If you use [mise](https://mise.jdx.dev/), `mise install` installs the pinned golangci-lint, and the tasks below are available as `mise run <task>`.

## Build, vet, test, lint

CI runs these commands on every pull request, on the Go version in `go.mod` and on the latest stable Go:

```bash
go build ./...
go vet ./...
go vet -tags integration ./...
go test ./... -short -count=1 -race -v
golangci-lint run ./...
```

It also fails if `go mod tidy` changes `go.mod` or `go.sum`.

The matching mise tasks are `build`, `vet`, `test-race`, and `lint`. `mise run ci` runs build, vet, lint, and test in one go. Use `mise run fmt` (`gofmt -s -w .`) to format code.

Unit tests must not call real provider APIs; use local HTTP fixtures. Tests that call real APIs go in `*_integration_test.go` files with the `integration` build tag and a `TestIntegration_` name prefix. They send billed requests and run only when asked for:

```bash
go test -tags integration -count=1 -run '^TestIntegration_' ./...
```

Credentials are read from the environment or from a `.env` file in the repository root. See [`.env.example`](.env.example) for the variables.

## Commit messages

Commits follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <summary>
```

Common types in this repository are `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `ci`, and `chore`. The scope is usually the package or provider, for example `sdk`, `provider`, `openai`, `anthropic`, or `google`. Mark breaking changes with `!` after the type or scope, for example `refactor(sdk)!: ...`, and describe the change in the commit body.

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Keep each pull request focused on one change. Add or update tests for behavior changes.
3. Update `README.md` and `docs/` when you change public API or provider behavior.
4. Run the commands above locally and make sure they pass.
5. Open a pull request against `main`. Use a Conventional Commit summary as the title. Pull requests are usually squash-merged, and the title becomes the commit message on `main`.

A maintainer reviews the pull request. CI must pass before it is merged.
