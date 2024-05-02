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

const FIND_SAMPLES_SQL = `SELECT uuid, array, name
	FROM samples WHERE array = ?1 AND name LIKE ?2 ORDER BY array, name`

type MicroarrayDB struct {
	Path            string
	Db              *sql.DB
	AllSamplesStmt  *sql.Stmt
	FindSamplesStmt *sql.Stmt
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

type Sample struct {
	Uuid  string
	Array string
	Name  string
}

func NewMicroarrayDb(dir string) (*MicroarrayDB, error) {
	db := sys.Must(sql.Open("sqlite3", path.Join(dir, "samples.db")))

	return &MicroarrayDB{Db: db,
		Path:            dir,
		AllSamplesStmt:  sys.Must(db.Prepare(ALL_SAMPLES_SQL)),
		FindSamplesStmt: sys.Must(db.Prepare(FIND_SAMPLES_SQL)),
	}, nil
}

func (microarraydb *MicroarrayDB) AllSamples() (*[]Sample, error) {

	rows, err := microarraydb.AllSamplesStmt.Query()

	if err != nil {
		return nil, err
	}

	return rowsToSamples(rows)
}

func (microarraydb *MicroarrayDB) FindSamples(array string, search string) (*[]Sample, error) {

	rows, err := microarraydb.FindSamplesStmt.Query(array, fmt.Sprintf("%%%s%%", search))

	if err != nil {
		return nil, err
	}

	return rowsToSamples(rows)
}

func (microarraydb *MicroarrayDB) Expression(array string, samples []string) (*ExpressionData, error) {
	// let sample_ids = vec![
	//     "0c3b8a19-1975-4c6e-aece-44a59c71719d",
	//     "0c4f0c89-af16-484a-a408-8dfde25d8f10",
	// ];

	nSamples := len(samples)

	//eprintln!("{:?}", Path::new(&self.path).join("meta.tsv").to_str());
	// open meta data
	file, err := os.Open(path.Join(microarraydb.Path, array, "meta.tsv"))

	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	reader.Comma = '\t'

	columnValues, err := reader.Read()

	if err != nil {
		return nil, err
	}

	probeIds := columnValues[1:]

	nProbes := len(probeIds)

	columnValues, err = reader.Read()

	if err != nil {
		return nil, err
	}

	entrezIds := columnValues[1:]

	columnValues, err = reader.Read()

	if err != nil {
		return nil, err
	}

	geneSymbols := columnValues[1:]

	rowRecords := make([][]float64, nSamples)
	sampleNames := make([]string, nSamples)

	// let mut rdr = csv::ReaderBuilder::new()
	//     .has_headers(true)
	//     .delimiter(b'\t')
	//     .from_reader(file);

	for _, sample := range samples {
		file, err := os.Open(path.Join(microarraydb.Path, fmt.Sprintf("%s.tsv", sample)))

		if err != nil {
			return nil, err
		}

		reader := csv.NewReader(file)
		reader.Comma = '\t'

		columnValues, err := reader.Read()

		if err != nil {
			return nil, err
		}

		sampleNames = append(sampleNames, columnValues[0])

		for {
			columnValues, err := reader.Read()

			if err == io.EOF {
				break
			}

			if err != nil {
				return nil, err
			}

			rowRecords = append(rowRecords, sys.Map(columnValues[1:], func(s string) float64 {
				v, err := strconv.ParseFloat(s, 64)

				if err != nil {
					return 0
				}

				return v
			}))
		}
	}

	data := ExpressionData{
		Exp:    make([][]float64, nProbes),
		Header: sampleNames,
		Index: ExpressionDataIndex{
			ProbeIds:    probeIds,
			EntrezIds:   entrezIds,
			GeneSymbols: geneSymbols,
		},
	}

	// We are going to transpose the data we read into this output
	// array
	//let mut data:Vec<Vec<f64>> = vec![vec![0.0; n_samples]; n_probes] ;

	for row := range nProbes {
		data.Exp[row] = make([]float64, nSamples)

		for col := range nSamples {
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

func rowsToSamples(rows *sql.Rows) (*[]Sample, error) {
	var uuid string
	var array string
	var name string

	samples := []Sample{}

	defer rows.Close()

	for rows.Next() {

		err := rows.Scan(&uuid, &array, &name)

		if err != nil {
			fmt.Println(err)
		}

		samples = append(samples, Sample{uuid, array, name})
	}

	return &samples, nil
}
