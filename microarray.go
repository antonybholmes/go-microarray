package microarray

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/antonybholmes/go-sys"
)

const ALL_SAMPLES_SQL = `SELECT uuid, array, name
	FROM samples ORDER BY array, name`

type MicroarrayDB struct {
	Path           String
	Db             *sql.DB
	AllSamplesStmt *sql.Stmt
}

type ExpressionDataIndex struct {
	ProbeIds    []string
	EntrezIds   []string
	GeneSymbols []string
}

type ExpressionData struct {
	Exp    [][]float64
	Header []string
	Index  ExpressionDataIndex
}

func NewMicroarrayDb(file string) (*MicroarrayDB, error) {
	db := sys.Must(sql.Open("sqlite3", file))

	return &MicroarrayDB{Db: db,
		Path:           file,
		AllSamplesStmt: sys.Must(db.Prepare(ALL_SAMPLES_SQL)),
	}, nil
}

func (microarraydb *MicroarrayDB) Expression(samples []string) (*ExpressionData, error) {
	// let sample_ids = vec![
	//     "0c3b8a19-1975-4c6e-aece-44a59c71719d",
	//     "0c4f0c89-af16-484a-a408-8dfde25d8f10",
	// ];

	n_samples := len(samples)

	//eprintln!("{:?}", Path::new(&self.path).join("meta.tsv").to_str());
	// open meta data
	file, err := os.Open(path.Join(microarraydb.Path, "meta.tsv"))

	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	reader.Comma = '\t'

	columnValues, err := reader.Read()

	if err != nil {
		return nil, err
	}

	probe_ids := columnValues[1:len(columnValues)]

	n_probes := len(probe_ids)

	columnValues, err = reader.Read()

	if err != nil {
		return nil, err
	}

	entrez_ids := columnValues[1:len(columnValues)]

	columnValues, err = reader.Read()

	if err != nil {
		return nil, err
	}

	gene_symbols := columnValues[1:len(columnValues)]

	rowRecords := make([][]float64, n_samples)
	sample_names := make([]string, n_samples)

	// let mut rdr = csv::ReaderBuilder::new()
	//     .has_headers(true)
	//     .delimiter(b'\t')
	//     .from_reader(file);

	for _, sample_id := range samples {
		file, err := os.Open(path.Join(microarraydb.Path, fmt.Sprintf("%s.tsv", sample_id)))

		if err != nil {
			return nil, err
		}

		reader := csv.NewReader(file)
		reader.Comma = '\t'

		columnValues, err := reader.Read()

		if err != nil {
			return nil, err
		}

		sample_names = append(sample_names, columnValues[0])

		for {
			columnValues, err := reader.Read()
			if err != nil {
				break
			}
			if err == io.EOF {
				break
			}

			rowRecords = append(rowRecords, sys.Map(columnValues[1:len(columnValues)], func(s string) float64 {
				v, err := strconv.ParseFloat(s, 64)

				if err != nil {
					return 0
				}

				return v
			}))
		}
	}

	data := ExpressionData{
		Exp:    make([][]float64, n_probes),
		Header: sample_names,
		Index: ExpressionDataIndex{
			probe_ids,
			entrez_ids,
			gene_symbols,
		},
	}

	// We are going to transpose the data we read into this output
	// array
	//let mut data:Vec<Vec<f64>> = vec![vec![0.0; n_samples]; n_probes] ;

	for row := range n_probes {
		for col := range n_samples {
			data.Exp[row][col] = rowRecords[col][row]
		}

		//eprintln!("{:?} {} out_row", out_row, n_samples);

		// wtr.write_record(&out_row)?;
	}

	// for result in rdr.records() {
	//     //for row in data {
	//     let record =
	//         result.map_err(|_| MicroarrayError::FileError("header issue".to_string()))?;

	//     let row = cols.iter().map(|c| &record[**c]).collect::<Vec<&str>>();
	//     //println!("{}", &record[0]);
	//     wtr.write_record(&row)
	//         .map_err(|_| MicroarrayError::FileError("header issue".to_string()))?;
	// }

	//let vec: Vec<u8> = wtr.into_inner()?;

	//let data = String::from_utf8(vec)?;

	return &data, nil
}

func (microarraydb *MicroarrayDB) Close() {
	microarraydb.Db.Close()
}
