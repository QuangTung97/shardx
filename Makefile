.PHONY: test lint install-tools

test:
	go test ./...

lint:
	go fmt ./...
	golint ./...
	go vet ./...
	errcheck ./...
	gocyclo -over 10 .

install-tools:
	go install github.com/matryer/moq
	go install golang.org/x/lint/golint
	go install github.com/kisielk/errcheck
	go get github.com/fzipp/gocyclo/cmd/gocyclo
