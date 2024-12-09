package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERRO" // level = 3
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// Custom topics with distinct colors
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APLY"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	level := getTopicLevel(DError) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logLevel int

// ANSI color codes for better visualization
const (
	reset     = "\033[0m"
	red       = "\033[31m"
	yellow    = "\033[33m"
	green     = "\033[32m"
	blue      = "\033[34m"
	magenta   = "\033[35m"
	cyan      = "\033[36m"
	white     = "\033[37m"
	lightRed  = "\033[91m"
	lightCyan = "\033[96m"
)

func init() {
	logLevel = getEnvLevel()
	logStart = time.Now()

	// Remove default log date and time
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Color mapping for each topic
func getColorForTopic(topic logTopic) string {
	switch topic {
	case DError:
		return red
	case DWarn:
		return yellow
	case DInfo:
		return green
	case DDebug:
		return blue
	case DClient:
		return cyan
	case DCommit:
		return magenta
	case DDrop:
		return lightRed
	case DLeader:
		return lightCyan
	case DLog:
		return green
	case DLog2:
		return yellow
	case DPersist:
		return blue
	case DSnap:
		return magenta
	case DTerm:
		return cyan
	case DTest:
		return white
	case DTimer:
		return lightRed
	case DTrace:
		return lightCyan
	case DVote:
		return yellow
	case DApply:
		return lightCyan
	default:
		return reset
	}
}

func LOG(peerId int, term int, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	if logLevel <= topicLevel {
		timeElapsed := time.Since(logStart).Microseconds()
		timeElapsed /= 100
		color := getColorForTopic(topic)
		prefix := fmt.Sprintf("%s%06d | T%04d | %-5s | S%d%s ", color, timeElapsed, term, string(topic), peerId, reset)
		format = prefix + format
		log.Printf(format, a...)
	}
}
