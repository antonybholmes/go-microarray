package microarraydb

import (
	"errors"

	"github.com/antonybholmes/go-microarray"
)

// pretend its a global const
var instance *microarray.MicroarrayDB

func InitDB(path string) error {
	var err error

	instance, err = microarray.NewMicroarrayDb(path)

	return err
}

func FindSamples(platform *microarray.Platform, search string) (*microarray.MicroarraySamples, error) {
	if instance == nil {
		return nil, errors.New("microarray db not initialized")
	}

	return instance.FindSamples(platform, search)
}

func Expression(samples *microarray.MicroarraySamplesReq) (*microarray.ExpressionData, error) {
	if instance == nil {
		return nil, errors.New("microarray db not initialized")
	}

	return instance.Expression(samples)
}
