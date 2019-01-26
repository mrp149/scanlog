



#scanlog = scandecl.go scanscan1.go mainscan.go
scanlog = main.go scaner.go


all: scanlog

scanlog: $(scanlog)

	go build -o $@ $(scanlog)


run:  $(scanlog)
	go run $(scanlog) < logs

fmt:  $(scanlog)
	go fmt $(scanlog)

