package microarray

import (
	"database/sql"

	"github.com/antonybholmes/go-sys"
)

const ALL_SAMPLES_SQL = `SELECT uuid, array, name
	FROM samples ORDER BY array, name`

type MicroarrayDB struct {
	db             *sql.DB
	allSamplesStmt *sql.Stmt
}

func NewMicroarrayDb(file string) (*MicroarrayDB, error) {
	db := sys.Must(sql.Open("sqlite3", file))

	return &MicroarrayDB{db: db,
		allSamplesStmt: sys.Must(db.Prepare(ALL_SAMPLES_SQL)),
	}, nil
}

func  (microarraydb *MicroarrayDB) Expression(samples []string) (ExpressionData, error) {
	// let sample_ids = vec![
	//     "0c3b8a19-1975-4c6e-aece-44a59c71719d",
	//     "0c4f0c89-af16-484a-a408-8dfde25d8f10",
	// ];

	n_samples := len(samples)

	//eprintln!("{:?}", Path::new(&self.path).join("meta.tsv").to_str());
	// open meta data
	let file, err := os.Open(Path::new(&self.path).join("meta.tsv"))?;

	//eprintln!("{:?}", file.metadata());

	let mut rdr = csv::ReaderBuilder::new()
		.has_headers(false)
		.delimiter(b'\t')
		.from_reader(file);

	let mut record: StringRecord = StringRecord::new();
	//let mut probe_ids: StringRecord = StringRecord::new();
	rdr.read_record(&mut record)?;


	let probe_ids: Vec<String> = record.iter().skip(1).map(|v| v.to_string()).collect();
	let n_probes: usize = probe_ids.len();

	rdr.read_record(&mut record)?;
	let entrez_ids: Vec<String> = record.iter().skip(1).map(|v| v.to_string()).collect();

	rdr.read_record(&mut record)?;
	let gene_symbols: Vec<String> = record.iter().skip(1).map(|v| v.to_string()).collect();

	let mut row_records: Vec<Vec<f64>> = Vec::with_capacity(n_samples);
	let mut samples_names: Vec<String> = Vec::with_capacity(n_samples);

	// let mut rdr = csv::ReaderBuilder::new()
	//     .has_headers(true)
	//     .delimiter(b'\t')
	//     .from_reader(file);

	for sample_id in sample_ids.iter() {
		let file = File::open(Path::new(&self.path).join(format!("{}.tsv", sample_id)))?;

		rdr = csv::ReaderBuilder::new()
			.has_headers(true)
			.delimiter(b'\t')
			.from_reader(file);

		let mut row: StringRecord = StringRecord::new();
		rdr.read_record(&mut row)?;
		samples_names.push(row[0].to_string());

		// read data into array
		let values = row
			.iter()
			.skip(1)
			.map(|v| v.parse::<f64>().unwrap())
			.collect::<Vec<f64>>();

		row_records.push(values);
	}

	let mut table = ExpressionData {
		exp: vec![vec![0.0; n_samples]; n_probes],
		header: samples_names,
		index: ExpressionDataIndex {
			probe_ids,
			entrez_ids,
			gene_symbols,
		},
	};

	// We are going to transpose the data we read into this output
	// array
	//let mut data:Vec<Vec<f64>> = vec![vec![0.0; n_samples]; n_probes] ;

	for row in 0..n_probes {
		for col in 0..n_samples {
			table.exp[row][col] = row_records[col][row];
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

	Ok(table)
}

func (microarraydb *MicroarrayDB) Close() {
	microarraydb.db.Close()
}
