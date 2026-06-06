.PHONY: build test race bench demo clean

build:
	go build -o raft-kv .

test:
	go test -v -timeout 60s ./raft/ ./store/

race:
	go test -v -race -timeout 120s ./raft/ ./store/

bench:
	go test -bench=. -benchtime=10s -timeout 120s ./benchmark/

demo: build
	./raft-kv demo

clean:
	rm -f raft-kv
