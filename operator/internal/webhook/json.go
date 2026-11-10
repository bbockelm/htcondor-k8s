package webhook

import "encoding/json"

// jsonMarshal is a seam: the admission response is a JSON patch computed
// against the raw request bytes, so the claimed object has to be re-encoded
// with the same encoder the API server used.
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
