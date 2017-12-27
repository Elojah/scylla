PACKAGE  = scylla
DATE    ?= $(shell date +%FT%T%z)
VERSION ?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || \
			cat $(CURDIR)/.version 2> /dev/null || echo v0)

GOBIN   = $(GOPATH)/bin
GODOC   = godoc
GOFMT   = gofmt

V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[0;35m▶\033[0m")

.PHONY: all

all: lint test

# Tests
.PHONY: deps
deps:
	$Q go get -u github.com/golang/dep/cmd/dep
	dep ensure -vendor-only

.PHONY: test
test:
	go test -cover -race -v ./...

.PHONY: lint
lint: $(GOLINT) ; $(info $(M) running gometalinter…) @
	$Q GO_VENDOR=1 gometalinter "--vendor" \
					"--disable=gotype" \
					"--disable=vetshadow" \
					"--disable=gocyclo" \
					"--fast" \
					"--json" \
					"./..." \

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	@ret=0 && for d in $$(go list -f '{{.Dir}}' ./... | grep -v /vendor/); do \
		$(GOFMT) -l -w $$d/*.go || ret=$$? ; \
	 done ; exit $$ret


.PHONY: version
version:
	@echo $(VERSION)
