

scanlog = main.go scaner.go


all: scanlog

scanlog: $(scanlog)

	go build -o $@ $(scanlog)


run:  $(scanlog)
	go run $(scanlog) top  one logs

fmt:  $(scanlog)
	go fmt $(scanlog)

