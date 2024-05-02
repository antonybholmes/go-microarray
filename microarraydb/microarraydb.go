package microarraydb

import (
	"fmt"

	"github.com/antonybholmes/go-microarray"
)

// pretend its a global const
var instance *microarray.MicroarrayDB

func InitDB(path string) error {
	var err error

	instance, err = microarray.NewMicroarrayDb(path)

	return err
}

func FindSamples(array string, search string) (*[]microarray.MicroarraySample, error) {
	if instance == nil {
		return nil, fmt.Errorf("microarray db not initialized")
	}

	return instance.FindSamples(array, search)
}

func Expression(samples *microarray.MicroarraySamplesReq) (*microarray.ExpressionData, error) {
	if instance == nil {
		return nil, fmt.Errorf("microarray db not initialized")
	}

	return instance.Expression(samples)
}
