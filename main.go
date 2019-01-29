package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

var debug bool = false

//
// by mikhailp at acm dot org
//

type Params struct {
	act        bool
	top        bool
	rate       bool
	rep        bool
	bad        bool
	maxtop     int
	fromTime   int64 // the time in unix format
	toTime     int64 // the time in unix format
	fromDate   string
	toDate     string
	rangeDate  string
	inputFiles []string
	outputFile string
}

var args *Params = new(Params)

const UNIX = "Jan _2 15:04:05 MST 2006"
const TEST5 = "January _2 15:04:05 MST 2006"
const TEST4 = "January _2 15:04:05 2006"
const TEST3 = "January _2 2006"
const TEST2 = "January _2"

type dateFmt struct {
	dstr   string
	format string
	nfmt   int
	year   string
	time   time.Time
}

func scan_time(dt dateFmt) int64 {
	if debug {
		fmt.Println("scan", dt)
		fmt.Printf("dstr=%s\n", dt.dstr)
		fmt.Printf("format=%s\n", dt.format)
	}
	if dt.dstr == "" {
		fmt.Fprintln(os.Stdout, "input expected")
		return 0
	}
	f := dt.format
	d := dt.dstr
	tm, err := time.Parse(f, d)
	dt.time = tm
	if err == nil {
		if debug {
			fmt.Println(tm)
		}
		dt.year = fmt.Sprint(tm.Year())
		re := time.Time.Unix(tm)
		if debug {
			fmt.Printf("%8x\n", re)
		}
		return re
	}

	if err != nil {
		fmt.Fprintln(os.Stdout, "bad time format:", d, err)
	}

	return 0

}

var from, to dateFmt

// make a guess on the date format
//
// The supported formats and "from-to" combinations
//
// A: "January 12 15:04:05 MST 2006"
// B: "January 12 15:04:05 2006"
// C: "January 12 2006"
// D: "January 12"
//
//  from {D}      to {A|B|C}   from will have the same year as to
//  from {A|B|C}  to {A|B|C}
//  from {D}      to {D}       assuming the current year

func guess_format(date *string) *dateFmt {

	var f, l string
	var s string = ""
	var n = 0
	guess := new(dateFmt)

	// repack the date and then make a guess what is the format
	for _, t := range strings.Split(*date, " ") {
		if t != "" {
			if s != "" {
				s += " "
			}
			s += t
			l = t
			n++
		}
	}

	if debug {
		fmt.Println("n=", n)
	}
	switch n {
	case 2:
		f = TEST2
	case 3:
		f = TEST3
	case 4:
		f = TEST4
	case 5:
		f = TEST5
	default:
		f = "UNKNOWN"
		n = 0
	}

	guess.dstr = s
	guess.format = f
	guess.nfmt = n
	guess.year = ""
	if n >= 3 {
		guess.year = l // year ?
	}
	if debug {
		fmt.Println("guessed format", guess)
	}
	return guess
}

// process all date information

func makedates() {

	var now time.Time = time.Now()

	// from, to and past combinations
	//    from to:   from  -> to
	//    past:      now-past -> now
	//    from past: from-past -> from  !! rejected
	//    to past:   to-past -> to    !! rejected

	// from, to and past combinations

	if *toparam == "" && *fromparam == "" { // no from-to, using only past, or past-default

		//past case
		duration, err := time.ParseDuration(*past)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		seconds := time.Time.Unix(now)
		args.toTime = seconds
		delta := (duration.Nanoseconds() / 1000000000)
		args.fromTime = seconds - delta
		args.toDate = fmt.Sprint(now)
		dt := -time.Duration(delta)
		args.fromDate = fmt.Sprint(now.Add(time.Second * dt))

	} else {

		from := guess_format(fromparam)
		to := guess_format(toparam)

		//from-to case
		if to.nfmt > 0 && from.nfmt > 0 {
			if debug {

				fmt.Println(now.Zone())
			}
			//from-to with no year field
			if to.nfmt == 2 && from.nfmt == 2 { // add current year
				from.dstr += (" " + fmt.Sprint(now.Year()))
				from.format = TEST3
				from.nfmt++
				to.dstr += (" " + fmt.Sprint(now.Year()))
				to.format = TEST3
				to.nfmt++
			}

			t := scan_time(*to)
			if t == 0 {
				fmt.Fprintln(os.Stderr, "to format looks wrong")
			}
			args.toTime = t
			args.toDate = fmt.Sprint(to.time)

			//from-to case from="January 1" to="January 12 2018"
			if from.nfmt == 2 { // no year?
				if to.nfmt >= 3 { // the same year from to, if it has one
					from.dstr += (" " + to.year)
					from.format = TEST3
					from.nfmt++
					from.year = to.year
				}
			}
			t = scan_time(*from)
			if t == 0 {
				fmt.Fprintln(os.Stderr, "from format looks wrong")
			}
			args.fromTime = t
			args.fromDate = fmt.Sprint(from.time)
		}

		if *toparam != "" && *fromparam == "" { // "to" and "past" used
			if *past == "" {
				fmt.Fprintln(os.Stderr, "past flag is expected")
			}
			// ideally past-to and past-from combinations could represents
			// time intervals defined as {to-past, to} and {from, from+past}
			fmt.Fprintln(os.Stderr, "past-to combination not implemented")
		}
	}

	if args.fromTime > args.toTime {
		fmt.Fprintln(os.Stderr, "wrong range in dates: start after end!")
	}
}

func setup_defaults() {
	a := args

	a.act = false
	a.top = false
	a.rate = false
	a.rep = false
	a.bad = false
	a.maxtop = 5
}

func set_defaults() Params {
	return Params{
		act:    false,
		top:    false,
		rate:   false,
		rep:    false,
		maxtop: 5,
	}
}

func init() {
}

var (
	options   = flag.NewFlagSet("options", flag.ContinueOnError)
	fromparam = options.String("from", "", "set starting date of the filter")
	toparam   = options.String("to", "", "set ending date of the filter")
	past      = options.String("past", "15m", "define time interval from now to a past event, e.g. 1h15m")
	maxtop    = options.Int("m", 5, "set up cut-off limit in top-list reports")
	output    = options.String("o", "", "redirect output to the listed file")
	help      = options.Bool("h", false, "print help information")
)

func main() {
	code := run()
	if code != 0 {
		os.Exit(code)
	}
}

func usage() {
	usage := "Usage:  <report> [<time options>] [<flags>] [logfile1] ...\n\n"
	usage += "Reports:\n"
	usage += "  top\treport most active users on the server\n"
	usage += "  act\tprint activity report on the build server\n"
	usage += "  bad\tprint report on crashed and canceled jobs\n"
	usage += "  rate\tcalculate the success rate in builds\n"
	usage += "  rep\tprint standard report: top users and activity\n\n"
	usage += "Options and flags:\n"
	fmt.Fprintf(os.Stderr, usage)
	options.PrintDefaults()
}

func run() int {

	options.SetOutput(os.Stderr)
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
		return 1
	}

	setup_defaults()

	switch os.Args[1] {

	// build activity
	case "act":
		args.act = true
		args.rate = true

		// top users
	case "top":
		args.top = true

		// error rate
	case "rate":
		args.rate = true

	case "bad":
		args.bad = true

		// reports
	case "rep":
		args.top = true
		args.rate = true
		args.act = true
		args.rep = true
	default:
		flag.Usage()
		return 1
	}
	err := options.Parse(os.Args[2:])
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	if *help {
		usage()
		tips := "Examples:\n\n"
		tips += "  top -from \"January 1\" -to \"January 20 2019\" -m 10 \n"
		tips += "  top -from \"January 3 2018\" -to \"January 20 2019\" -m 1 \n"
		tips += "  act -past 1h10m\n"
		tips += "  rate -m 10 -from \"December 31 23:59:59 2001\" -to \"January 20 10:11:11 2019\" \n"
		tips += "  rep -past 100000s -m 3\n"
		tips += " <tbd>\n"

		fmt.Fprintf(os.Stderr, tips)
		return 1
	}

	args.maxtop = *maxtop
	args.inputFiles = options.Args()
	args.outputFile = *output

	makedates()

	err = runrun()
	if err != nil {
		fmt.Println("Error: \n", err)
		return 1
	}
	return 0
}

func runrun() (err error) {

	initscaner()

	if debug {
		fmt.Println("start filtering ...")

		fmt.Println(args)

		fmt.Println("filtering", len(args.inputFiles), "files")
	}
	if len(args.inputFiles) != 0 { // no file, only stdin
		for _, in := range args.inputFiles {
			_, err := os.Stat(in)
			if err != nil {
				return err
			}
		}

		for _, in := range args.inputFiles {
			f, err := os.Open(in)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			if debug {
				fmt.Println("filtering file:", in)
			}
			scanlogfile(f)
			f.Close()
		}
	} else { //stdin
		if debug {
			fmt.Println("filtering STDIN")
		}

		scanlogfile(os.Stdin)

	}

	if debug {
		fmt.Println("running report ...")
	}

	if args.outputFile != "" {
		f, err := os.Create(args.outputFile)
		if err != nil {
			return err
		}
		os.Stdout = f
		reporter()
		f.Close()
	} else {
		reporter()
	}
	return nil
}
