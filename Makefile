empty:
	$(info Select a target)

all: dep build

dep:
	go get -d

build:
	go build -o parser.so -buildmode=c-shared parser.go
