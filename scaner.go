package main

//
// by mikhailp at acm dot org
//
// TODO
//

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const RFC3339 = "2006-01-02T15:04:05Z07:00"

type Id128 struct {
	hi, lo uint64
}

type Rec struct {
	jid        Id128 // job id
	uid        int   // index in usermap[]
	tj, ts, te int64 // unixtime for tj, ts,te
	exit       int   // exit code
	cancel     bool  // cancel
	imgsz      int64 // build size
}

type ListItem struct {
	item *Rec
	next *ListItem
}

type List struct {
	head *ListItem
	tail *ListItem
}

func (list *List) Init() {
	list.head = nil
	list.tail = nil

}

func (list *List) Insert(i *Rec) {
	item := new(ListItem)
	item.item = i
	item.next = list.head
	list.head = item
	if list.tail == nil {
		list.tail = item
	} else {
		list.tail = item.next
	}
}

func (list *List) Append(i *Rec) {
	item := new(ListItem)
	item.item = i
	item.next = nil

	if list.head == nil {
		list.head = item
		return
	}

	if list.tail == nil {
		list.tail = item
		(*list.head).next = item
		return
	}
	(*list.tail).next = item
	list.tail = item
}

func (list *List) Head() *ListItem {
	return list.head
}

func (list *List) Next(i *ListItem) *ListItem {
	return i.next
}

func (list *List) Item(i *ListItem) *Rec {
	return i.item
}

const (
	L_JOBID = iota
	L_USERID
	L_JOBINIT
	L_JOBSTART
	L_JOBEND
	L_CANCEL
	L_EXIT
	L_IMGSZ
)

//  define the format of log file  index->type
var Log_format = []int{
	L_JOBID,
	L_USERID,
	L_JOBINIT,
	L_JOBSTART,
	L_JOBEND,
	L_CANCEL,
	L_EXIT,
	L_IMGSZ,
}

// linked list of records
var list *List

// users with counters
var users map[string]int

type userEntry struct {
	name  string // user id from log file
	id    int    // assigned id
	jobs  int    // jobs submitted
	exit  int    // bad exits
	time  int    // consumed time
	space int64  // consume space
}

type Totals struct {
	active    int
	total     int
	submitted int
	running   int
	started   int
	ended     int
	crashed   int
	canceled  int
	timeused  int
	spaceused int64
}

var tot Totals

var usermap = make([]userEntry, 0)
var userId int = 0

func username(id int) string {
	return usermap[id].name
}

func scanlogfile(file *os.File) {

	var i int
	var line string
	var fld [8]string
	var jid Id128
	var uid string
	var imgsz int64
	var exit int
	var cancel bool

	scanner := bufio.NewScanner(file)

	rc := new(Rec)

	for scanner.Scan() {
		line = scanner.Text()

		nf := 0
		ignore := 0

		for _, tok := range strings.Split(string(line), ",") {
			if nf >= len(Log_format) || tok == "" {
				nf = len(Log_format) + 1
				break
			}
			fld[nf] = tok
			nf++
		}

		if nf != len(Log_format) {
			fmt.Fprintln(os.Stderr, "Found bad record! <", line, "> Ignoring!")
			ignore++
			continue
		}

		for _, i = range Log_format {
			switch Log_format[i] {
			// jobid
			case L_JOBID:
				{
					n, err := fmt.Sscanf(fld[i], "%16x%16x", &jid.hi, &jid.lo)

					if err != nil || n != 2 {
						fmt.Fprintln(os.Stderr, "bad jobid field:", fld[i], err)
						ignore++
					}
					rc.jid = jid
				}
			// userid
			case L_USERID:
				{
					n, err := fmt.Sscanf(fld[i], "%s", &uid)

					if err != nil || n != 1 || uid == "" {
						fmt.Fprintln(os.Stderr, "bad userid field:", fld[i], err)
						ignore++
					}
					// rc.uid = uid
				}

			// job submission time
			case L_JOBINIT:
				{
					tj, err := time.Parse(RFC3339, fld[i])
					if err != nil {
						fmt.Fprintln(os.Stderr, "bad time format in submit:", fld[i], err)
						ignore++
					}
					rc.tj = time.Time.Unix(tj)
				}
			// begin of build
			case L_JOBSTART:
				{
					ts, err := time.Parse(RFC3339, fld[i])
					if err != nil {
						fmt.Fprintln(os.Stderr, "bad time format in start:", fld[i], err)
						ignore++
					}
					rc.ts = time.Time.Unix(ts)
				}
			// end of build
			case L_JOBEND:
				{
					te, err := time.Parse(RFC3339, fld[i])
					if err != nil {
						fmt.Fprintln(os.Stderr, "bad time format in end:", fld[i], err)
						ignore++
					}
					rc.te = time.Time.Unix(te)
				}
			// cancel flag
			case L_CANCEL:
				{
					n, err := fmt.Sscanf(fld[i], "%t", &cancel)
					if err != nil || n != 1 {
						fmt.Fprintln(os.Stderr, "bad cancel field:", fld[i], err)
						ignore++
					}
					rc.cancel = cancel
				}
			// exit code
			case L_EXIT:
				{
					n, err := fmt.Sscanf(fld[i], "%d", &exit)
					if err != nil || n != 1 {
						fmt.Fprintln(os.Stderr, "bad exit field:", fld[i], err)
						ignore++
					}
					rc.exit = exit
				}

			// image size
			case L_IMGSZ:
				{
					n, err := fmt.Sscanf(fld[i], "%d", &imgsz)
					if err != nil || n != 1 {
						fmt.Fprintln(os.Stderr, "bad imagesize field:", fld[i], err)
						ignore++
					}
					rc.imgsz = imgsz
				}
			} // end switch
		}

		if ignore != 0 {
			if debug {
				sngl := "field"
				if ignore > 1 {
					sngl = "fields"
				}
				fmt.Fprintln(os.Stdout, "Found", ignore, "bad", sngl, "in the record! Ignoring!")
			}
			continue
		}

		// check for time wraps
		if rc.tj > rc.ts || rc.tj > rc.te || rc.ts > rc.te { // time wrap
			if debug {
				fmt.Println("ignored time wrap")
			}
			continue
		}
		// build the list of filtered records

		if check_time_range(rc, args.fromTime, args.toTime) {
			// counts per user
			if users[uid] == 0 {

				nu := new(userEntry)

				nu.name = uid
				nu.id = userId
				rc.uid = userId
				users[uid] = userId + 1

				usermap = append(usermap, *nu)
				userId++
				tot.active = userId
			} else {

				rc.uid = users[uid] - 1
			}
			list.Append(rc)
			rc = new(Rec)

		} else {
			if debug {
				fmt.Println("ignored by time range")
			}
		}
	}
	// if any problem?
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error on reading:", err)
	}

}

func test_user_mapping() {

	fmt.Println("users")
	for u, c := range users {
		fmt.Println(u, c)
	}
	for i := range usermap {
		fmt.Println(usermap[i], username(i), users[usermap[i].name])
	}
}

var test_fromtime, test_totime int64
var test_uid string

func init_test_condition() {
	var t time.Time
	test_uid = "5c00a8f685db9ec46dbc13d7"
	t, _ = time.Parse(RFC3339, "2018-10-01T14:46:16-04:00")
	test_fromtime = time.Time.Unix(t)
	t, _ = time.Parse(RFC3339, "2018-11-01T15:56:13-04:00")
	test_totime = time.Time.Unix(t)

}

func timecond(a int64, b int64, x int64, y int64) int {
	var l int64
	var r int
	if a <= x {
		if b > x {
			if b >= y {
				l = y - x
			} else {
				l = b - x
			}
		} else {
			l = 0
		}
	} else { // a > x
		if b > y {
			if a < y {
				l = y - a
			} else {
				l = 0
			}
		} else {
			l = b - a
		}
	}
	r = int(l)
	return r
}

func check_time_range(rp *Rec, fromtime int64, totime int64) bool {
	/*
	   - The marketing department wants to know how many builds were executed in a time window. For
	   example, how many builds were executed in the last 15 minutes, or in the last day, or between
	   January 1 and January 31, 2018.

	   - The marketing department wants to know which users are using the remote build service the
	   most. Who are the top 5 users and how many builds have they executed in the time window?

	   - The marketing department would like to know the build success rate, and for builds that are not
	   succeeding what are the top exit codes.
	*/

	//	tj := time.Time.Unix(r.tj)
	//	te := time.Time.Unix(r.te)

	if timecond(rp.tj, rp.te, fromtime, totime) > 0 {
		// before returning do accounif of the selected record

		// totals
		tot.total++

		//	submitted int
		if timecond(rp.tj, rp.tj+1, fromtime, totime) > 0 {
			tot.submitted++
		}
		//	running int
		tm := timecond(rp.ts, rp.te, fromtime, totime)
		if tm > 0 {
			tot.running++
			//	timused int
			//	spaceused int
			tot.timeused += (tm / 60)
			tot.spaceused += rp.imgsz

		}
		//	started int
		if timecond(rp.ts, totime+1, fromtime, totime) > 0 {
			tot.started++
		}
		//	ended int
		if timecond(rp.te, rp.te+1, fromtime, totime) > 0 {
			tot.ended++
		}
		//      cancel int
		if rp.cancel {
			tot.canceled++
		}
		//	crashed int
		if rp.exit != 0 {
			tot.crashed++
		}

		return true
	}
	return false
}

type TopEnt struct {
	k int
	v int
}

type TopByVal []TopEnt

func (a TopByVal) Len() int           { return len(a) }
func (a TopByVal) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TopByVal) Less(i, j int) bool { return a[i].v < a[j].v }

func count_success_rate(max int) {
	var rp *Rec
	var failed int = 0

	exits := make(map[int]int)

	toplist := make([]TopEnt, 0)

	total_records := 0
	for e := list.Head(); e != nil; e = list.Next(e) {
		total_records++
		rp = list.Item(e)
		//  the rate condition
		if timecond(rp.te, rp.te+1, args.fromTime, args.toTime) > 0 { // exits on error
			if rp.exit != 0 {
				if debug {
					fmt.Println("uid:", rp.uid, "user:", username(rp.uid), "cancel: ", rp.cancel, "exit: ", rp.exit, " image size:", rp.imgsz)
				}
				failed++
				exits[rp.exit]++
			}
		}
	}

	//top error codes

	for i := range exits {
		ent := new(TopEnt)
		ent.k = i        //error code
		ent.v = exits[i] //counter
		toplist = append(toplist, *ent)
	}

	if debug {
		fmt.Print(toplist)
	}
	sort.Sort(sort.Reverse(TopByVal(toplist)))
	if debug {
		fmt.Print(toplist)
	}

	succsess_rate := 100
	if total_records == 0 {
		succsess_rate = 0
	}

	if failed > 0 {
		succsess_rate = (total_records - failed) * 100 / total_records
	}

	fmt.Printf(" Succsess rate: %7d%%\n", succsess_rate)
	fmt.Printf("    jobs total:  %7d\n", total_records)

	if failed > 0 {

		fmt.Printf("    succseeded:  %7d\n", total_records-failed)
		fmt.Printf("        failed:  %7d\n", failed)
		header := "\nList of exit codes\n"
		header += " Rank Error  Count\n"
		header += "+----+-----+------+"
		fmt.Println(header)

		maxtop := max
		lasttop := 0

		for i := 0; i < len(toplist); i++ {

			if maxtop <= 0 && lasttop != toplist[i].v {
				break
			} else {
				lasttop = toplist[i].v

				fmt.Printf("%4d %5d %7d\n", i+1, toplist[i].k, toplist[i].v)
				maxtop--
			}
		}
	}
}

func count_top_users(max int) {
	var rp *Rec
	var toplist []TopEnt

	workmap := make([]userEntry, len(usermap))

	// collect statisitics
	for e := list.Head(); e != nil; e = list.Next(e) {
		rp = list.Item(e)

		if timecond(rp.tj, rp.tj+1, args.fromTime, args.toTime) > 0 { // submitions
			workmap[rp.uid].jobs++
		}
		tm := timecond(rp.ts, rp.te, args.fromTime, args.toTime)
		if tm > 0 { // time  and space was consumed
			workmap[rp.uid].time += tm
			workmap[rp.uid].space += rp.imgsz
		}
		if timecond(rp.te, rp.te+1, args.fromTime, args.toTime) > 0 { // exits on error
			if rp.exit != 0 {
				workmap[rp.uid].exit++
			}
		}
	}
	if debug {

		//	test_user_mapping()

		fmt.Println("usermap")
		for u, c := range users {
			fmt.Println(u, c)
		}
		fmt.Println("workmap")
		for i := range workmap {
			fmt.Println(username(i), users[usermap[i].name], workmap[i])
		}
	}
	//top submitters
	toplist = make([]TopEnt, 0)

	for i := range workmap {
		ent := new(TopEnt)
		ent.k = i // userid
		ent.v = workmap[i].jobs
		toplist = append(toplist, *ent)
	}

	if debug {
		fmt.Print(toplist)
	}
	sort.Sort(sort.Reverse(TopByVal(toplist)))
	if debug {
		fmt.Print(toplist)
	}
	if len(toplist) >= 0 {
		maxtop := max
		header := "\nList of most active users\n"
		header += " Rank      UserID                 Jobs\n"
		header += "+----+-------------------------+-----------+"
		fmt.Println(header)

		lasttop := 0
		for i := 0; i < len(toplist); i++ {

			if maxtop <= 0 && lasttop != toplist[i].v {
				break
			} else {
				lasttop = toplist[i].v

				fmt.Printf("%4d  %12s %12d\n", i+1, username(toplist[i].k), toplist[i].v)
				maxtop--
			}
		}
	}
	//top hours
	toplist = make([]TopEnt, 0)
	for i := range workmap {
		if workmap[i].time > 0 {
			ent := new(TopEnt)
			ent.k = i // userid
			ent.v = workmap[i].time
			toplist = append(toplist, *ent)
		}
	}

	sort.Sort(sort.Reverse(TopByVal(toplist)))

	if len(toplist) >= 0 {
		maxtop := max
		header := "\nList of most productive users\n"
		header += " Rank      UserID                 Time\n"
		header += "+----+-------------------------+----------+"
		fmt.Println(header)

		lasttop := 0
		for i := 0; i < len(toplist); i++ {

			if maxtop <= 0 && lasttop != toplist[i].v {
				break
			} else {
				lasttop = toplist[i].v
				fmt.Printf("%4d  %12s %12s\n", i+1, username(toplist[i].k),
					time.Duration.String(time.Second*time.Duration(toplist[i].v)))
				maxtop--
			}
		}
	}
	// troublemakers
	toplist = make([]TopEnt, 0)
	for i := range workmap {
		if workmap[i].exit > 0 {
			ent := new(TopEnt)
			ent.k = i // userid
			ent.v = workmap[i].exit
			toplist = append(toplist, *ent)
		}
	}

	sort.Sort(sort.Reverse(TopByVal(toplist)))

	if len(toplist) >= 0 {
		maxtop := max
		header := "\nList of most troubled users\n"
		header += " Rank      UserID                 Crashes\n"
		header += "+----+-------------------------+-----------+"
		fmt.Println(header)

		lasttop := 0
		for i := 0; i < len(toplist); i++ {

			if maxtop <= 0 && lasttop != toplist[i].v {
				break
			} else {
				lasttop = toplist[i].v

				fmt.Printf("%4d  %12s %12d\n", i+1, username(toplist[i].k), toplist[i].v)
				maxtop--
			}
		}
	}
}

func diskspace(value int64, toUnit string) string {
	toUnit = strings.TrimSuffix(strings.ToLower(toUnit), "s")

	if toUnit == "minimum" || toUnit == "auto" {
		switch {
		case value < 1024:
			toUnit = "b"
		case value < 1024*1024:
			toUnit = "kb"
		case value < 1024*1024*1024:
			toUnit = "mb"
		case value < 1024*1024*1024*1024:
			toUnit = "gb"
		case value < 1024*1024*1024*1024*1024:
			toUnit = "tb"
		default:
			toUnit = "pb"
		}
	}

	var output float64
	switch toUnit {
	default:
		output, toUnit = float64(value), "B"
	case "kb", "kbyte", "kilobyte":
		output, toUnit = float64(value)/1024, "kB"
	case "mb", "mbyte", "megabyte":
		output, toUnit = float64(value)/(1024*1024), "MB"
	case "gb", "gbyte", "gigabyte":
		output, toUnit = float64(value)/(1024*1024*1024), "GB"
	case "tb", "tbyte", "terabyte":
		output, toUnit = float64(value)/(1024*1024*1024*1024), "TB"
	case "pb", "pbyte", "petabyte":
		output, toUnit = float64(value)/(1024*1024*1024*1024*1024), "PB"
	}
	return fmt.Sprintf("%.3f", output) + toUnit
}

func report(r string, max int) {

	r = strings.ToUpper(r)
	fmt.Println("Report section:	", r, "\n")
	switch r {

	default:
		fmt.Println("unimplemented report type:", r)
		break
	case "TOP-USER":
		{

			count_top_users(max)
			if debug {
				fmt.Println("\nTop of Top:")

				count_top_users(1)
			}
		}

	case "JOB-COUNT":
		{

			fmt.Println("Statistic on build server\n")

			i := 0

			max = 100

			for e := list.Head(); e != nil; e = list.Next(e) {
				i++
				if debug {
					rp := list.Item(e)
					fmt.Println("uid:", rp.uid, "user:", username(rp.uid), "cancel: ", rp.cancel, "exit: ", rp.exit, " image size:", rp.imgsz)
				}
			}

			fmt.Println(" All totals are\n")
			fmt.Println(" recorded jobs:", tot.total)
			fmt.Println("     submitted:", tot.submitted)
			fmt.Println("       running:", tot.running)
			fmt.Println("       started:", tot.started)
			fmt.Println("      finished:", tot.ended)
			fmt.Println("       crashed:", tot.crashed)
			fmt.Println("      canceled:", tot.canceled)
			fmt.Println("     time used:", time.Duration.String(time.Second*time.Duration(tot.timeused)))
			fmt.Println("    space used:", diskspace(tot.spaceused, "auto"))
			fmt.Println("  active users:", tot.active)

			fmt.Println("\n    Total jobs:", i)
		}

	case "SUCCSESS-RATE":
		{
			count_success_rate(max)
		}

	case "SELECTED BAD":
		{
			var rp *Rec
			i := 0
			j := 0

			fmt.Println("Canceled and crashed jobs:\n")

			for e := list.Head(); e != nil; e = list.Next(e) {
				rp = list.Item(e)
				i++

				if rp.cancel || rp.exit != 0 {
					j++
					fmt.Println("uid:", rp.uid, "user:", username(rp.uid), "cancel: ", rp.cancel, "exit: ", rp.exit, " image size:", rp.imgsz)
				}
			}
			fmt.Println("\nFound", j, "canceled or crashed jobs in total of", i, "records")
		}

	}

	fmt.Println("\nEnd of section: ", r, "\n")
}

func initscaner() {
	// Create a new list
	list = new(List)
	list.Init()

	users = make(map[string]int)
}

func reporter() {

	max := args.maxtop
	fmt.Println("Report summary on build logs generated on:", time.Now(), "\n\nFilter setings:")
	fmt.Println("     From date:", args.fromDate, "\n       To date:", args.toDate, "\n  Maxtop limit:", args.maxtop)
	fmt.Println("\n")

	if args.act {
		report("Job-count", max)
	}
	if args.bad {
		report("SelecTed bad", max)
	}

	if debug {
		if args.top {
			report("topuser", max)
		}
	}
	if args.top {
		report("Top-user", max)
	}
	if args.rate {
		report("Succsess-rate", max)
	}
	fmt.Println("End of report.\n")
}
