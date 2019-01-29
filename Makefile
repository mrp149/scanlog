

scanlog = main.go scaner.go


all: scanlog

scanlog: $(scanlog)

	go build -o $@ $(scanlog)


run:  $(scanlog) log logs
	go run $(scanlog) top  log logs

fmt:  $(scanlog)
	go fmt $(scanlog)

