package compression

const (
	InvalidCompressionAlgorithm = iota
	CompressionAlgorithmArray
	CompressionAlgorithmDictionary
	CompressionAlgorithmGorilla
	CompressionAlgorithmDeltaDelta
)
