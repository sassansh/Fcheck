package main

import (
	"log"
	"math/rand"
	fchecker "sassansh/Fcheck/fcheck"
)

func main() {
	serverId := 1
	log.Printf("🔄 Starting server with ID %d", serverId)

	// Initialize fcheck
	notifyCh, err := fchecker.Initialize()
	if err != nil {
		log.Printf("Error initializing fcheck: %s", err)
		return
	}

	// Start responding to heartbeats
	fchecker.StartAcknowledging("127.0.0.1:45691")
	log.Printf("🫀 Responding to heartbeats now")

	// Start to monitor other server
	err = fchecker.AddNodeToMonitor(fchecker.MonitorNodeStruct{EpochNonce: uint64(rand.Int63()),
		HBeatLocalIPHBeatLocalPort: "127.0.0.1:0", HBeatRemoteIPHBeatRemotePort: "127.0.0.1:45692",
		LostMsgThresh: 10})
	if err != nil {
		log.Printf("Failed to add node to monitor %s", err)
	}

	// Start listening for failure notifications
	for {
		notification := <-notifyCh

		// Print notification
		log.Printf("🔴 Received failure notification: %s", notification)
	}

}
