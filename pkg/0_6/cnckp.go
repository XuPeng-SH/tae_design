package demo

type ICheckpointMgr interface {
	// Get a suitable checkpoint for the specified ts
	// Checkpoints: [100] --> [80] --> [30]
	// ts: 101, checkpoint: 100
	// ts: 90,  checkpoint: 80
	GetCheckpoint(ts Timestamp) Timestamp

	// Try add a new checkpoint
	// If not exists, return true else false
	AddCheckpoint(ts Timestamp) bool
	// Try remove a new checkpoint
	// If exists, return true else false
	PruneCheckpoint(ts Timestamp) bool

	// Get the min checkpoint timestamp
	// Timestamp.Minmum() if empty
	Min() Timestamp
	// Get the max checkpoint timestamp
	// Timestamp.Minmum() if empty
	Max() Timestamp

	// Get the count of checkpoints
	Count() int

	String() string
}
