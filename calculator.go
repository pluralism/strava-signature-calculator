package stravasignaturecalculator

import (
	"encoding/json"
	"github.com/pluralism/stravaminhashlsh"
	"io/ioutil"
	"log"
)

type randomCoefficients struct {
	Coefficients [][]uint64 `json:"coeffs"`
}

func NewCalculator() (*stravaminhashlsh.StravaSignatureCalculator, error) {
	minHash := stravaminhashlsh.NewStravaMinHash().
		ShingleSize(ShingleSize).
		Zoom(Zoom).
		Build()

	lsh := stravaminhashlsh.NewStravaLSH().
		Bands(Bands).
		Buckets(Buckets).
		StravaMinHash(minHash).
		Build()

	calculator := stravaminhashlsh.NewStravaSignatureCalculator().
		MinHash(minHash).
		LSH(lsh).
		Build()

	signatureSize := calculator.LSH.GetSignatureSize()
	err := calculator.Setup(getOrGenerateRandomCoefficients(signatureSize, calculator))
	if err != nil {
		return nil, err
	}

	return calculator, nil
}

func getOrGenerateRandomCoefficients(count uint, calculator *stravaminhashlsh.StravaSignatureCalculator) [][]uint64 {
	if fileExists(CoefficientsFile) {
		var randomCoefficients randomCoefficients
		file, _ := ioutil.ReadFile(CoefficientsFile)
		if err := json.Unmarshal(file, &randomCoefficients); err != nil {
			log.Fatal(err)
		}
		return randomCoefficients.Coefficients
	}

	coefficients := calculator.MinHash.GetRandomCoefficients(count)
	data, err := json.Marshal(coefficients)
	if err != nil {
		log.Fatal(err)
	}

	_ = ioutil.WriteFile(CoefficientsFile, data, 0644)
	return coefficients
}